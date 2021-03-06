<?php

namespace RedisInAction\Ch06;

use Ramsey\Uuid\Uuid;
use Predis\PredisException;
use Predis\Transaction\AbortedMultiExecException;
use RedisInAction\Helper\Threading;

function add_update_contact($conn, $user, $contact)
{
    $ac_list = 'recent:' . $user;
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->lrem($ac_list, $count = 0, $contact);
    $pipeline->lpush($ac_list, $contact);
    $pipeline->ltrim($ac_list, 0, 99);
    $pipeline->execute();
}

function remove_contact($conn, $user, $contact)
{
    $conn->lrem('recent:' . $user, $count = 0, $contact);
}

function fetch_autocomplete_list($conn, $user, $prefix)
{
    $candidates = $conn->lrange('recent:' . $user, 0, -1);
    $matches = [];
    foreach ($candidates as $candidate) {
        if (strpos(strtolower($candidate), strtolower($prefix)) === 0) {
            $matches[] = $candidate;
        }
    }

    return $matches;
}

const VALID_CHARACTERS = '`abcdefghijklmnopqrstuvwxyz{';

function find_prefix_range($prefix)
{
    $posn = strpos(VALID_CHARACTERS, substr($prefix, -1));
    $suffix = VALID_CHARACTERS[$posn > 0 ? ($posn - 1) : 0];

    return [
        substr($prefix, 0, -1) . $suffix . '{',
        $prefix . '{'
    ];
}

function autocomplete_on_prefix($conn, $guild, $prefix)
{
    list($start, $end) = find_prefix_range($prefix);
    $identifier = Uuid::uuid4()->toString();
    $start .= $identifier;
    $end   .= $identifier;

    $zset_name = 'members:' . $guild;

    $conn->zadd($zset_name, [$start => 0, $end => 0]);
    $trans = $conn->transaction(['cas' => true]);
    while (1) {
        try {
            $trans->watch($zset_name);

            $sindex = $trans->zrank($zset_name, $start);
            $eindex = $trans->zrank($zset_name, $end);
            $erange = min($sindex + 9, $eindex - 2);

            $trans->multi();

            $trans->zrem($zset_name, $start);
            $trans->zrem($zset_name, $end);
            $trans->zrange($zset_name, $sindex, $erange);

            $result = $trans->execute();

            $items = end($result);

            break;

        } catch (AbortedMultiExecException $e) {
            continue;
        }
    }

    return array_filter(
        $items,
        function ($item) {
            return strpos($item, '{') === false;
        }
    );
}

function join_guild($conn, $guild, $user)
{
    $conn->zadd('members:' . $guild, [$user => 0]);
}

function leave_guild($conn, $guild, $user)
{
    $conn->zrem('members:' . $guild, $user);
}

function acquire_lock($conn, $lockname, $acquire_timeout = 10)
{
    // when run in pthreads, uuid4 will trigger warning calling
    // feads() in paragonie/random_compat/lib/random_bytes_dev_urandom.php
    // PHP Warning: fread(): 122 is not a valid stream resource
    // using uuid1 here
    $identifier = Uuid::uuid1()->toString();

    $end = microtime(true) + $acquire_timeout;
    while (microtime(true) < $end) {
        if ($conn->setnx('lock:' . $lockname, $identifier)) {
            return $identifier;
        }

        usleep(1000);
    }

    return false;
}

function purchase_item_with_lock($conn, $buyerid, $itemid, $sellerid)
{
    $buyer = sprintf('users:%s', $buyerid);
    $seller = sprintf('users:%s', $sellerid);
    $item = sprintf('%s.%s', $itemid, $sellerid);
    $inventory = sprintf('inventory:%s', $buyerid);

    $locked = acquire_lock($conn, 'market:');
    if (!$locked) {
        return false;
    }

    $pipe = $conn->pipeline(['atomic' => true]);
    try {
        $pipe->zscore('market:', $item);
        $pipe->hget($buyer, 'funds');
        list($price, $funds) = $pipe->execute();
        if (is_null($price) OR $price > $funds) {
            return null;
        }

        $pipe->hincrby($seller, 'funds', intval($price));
        $pipe->hincrby($buyer, 'funds', intval(-$price));
        $pipe->sadd($inventory, $itemid);
        $pipe->zrem('market:', $item);
        $pipe->execute();

        return true;

    } finally {
        release_lock($conn, 'market:', $locked);
    }
}

function release_lock($conn, $lockname, $identifier)
{
    $trans    = $conn->transaction(['cas' => true]);
    $lockname = 'lock:' . $lockname;

    while (true) {
        try {
            $trans->watch($lockname);

            if ($trans->get($lockname) == $identifier) {
                $trans->multi();
                $trans->del($lockname);
                $trans->execute();

                return true;
            }

            $trans->unwatch();

            break;

        } catch (AbortedMultiExecException $e) {
            // pass
        }
    }

    return false;
}

function acquire_lock_with_timeout(
    $conn, $lockname, $acquire_timeout = 10, $lock_timeout = 10
)
{
    $identifier   = Uuid::uuid4()->toString();
    $lockname     = 'lock:' . $lockname;
    $lock_timeout = intval(ceil($lock_timeout));

    $end = microtime(true) + $acquire_timeout;
    while (microtime(true) < $end) {
        if ($conn->setnx($lockname, $identifier)) {
            $conn->expire($lockname, $lock_timeout);

            return $identifier;

        } elseif ($conn->ttl($lockname) < 0) {
            $conn->expire($lockname, $lock_timeout);
        }

        usleep(1000);
    }

    return false;
}

function acquire_semaphore($conn, $semname, $limit, $timeout = 0)
{
    $identifier = Uuid::uuid4()->toString();
    $now        = microtime(true);

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zremrangebyscore($semname, '-inf', $now - $timeout);
    $pipeline->zadd($semname, [$identifier, $now]);
    $pipeline->zrank($semname, $identifier);
    $result = $pipeline->execute();
    if (end($result) < $limit) {
        return $identifier;
    }

    $conn->zrem($semname, $identifier);

    return null;
}

function release_semaphore($conn, $semname, $identifier)
{
    return $conn->zrem($semname, $identifier);
}

function acquire_fair_semaphore($conn, $semname, $limit, $timeout = 10)
{
    $indentifier = Uuid::uuid4()->toString();
    $czset       = $semname . ':owner';
    $ctr         = $semname . ':counter';

    $now = microtime(true);

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zremrangebyscore($semname, '-inf', $now - $timeout);
    $pipeline->zinterstore($czset, [$czset, $semname], ['WEIGHTS' => [1, 0]]);

    $pipeline->incr($ctr);
    $result  = $pipeline->execute();
    $counter = end($result);

    $pipeline->zadd($semname, [$indentifier => $now]);
    $pipeline->zadd($czset, [$indentifier => $counter]);

    $pipeline->zrank($czset, $indentifier);
    $result = $pipeline->execute();
    if (end($result) < $limit) {
        return $indentifier;
    }

    $pipeline->zrem($semname, $indentifier);
    $pipeline->zrem($czset, $indentifier);
    $pipeline->execute();

    return null;
}

function release_fair_semaphore($conn, $semname, $identifier)
{
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zrem($semname, $identifier);
    $pipeline->zrem($semname . ':owner', $identifier);

    return $pipeline->execute()[0];
}

function refresh_fair_semaphore($conn, $semname, $identifier)
{
    if ($conn->zadd($semname, [$identifier, microtime(true)])) {
        release_fair_semaphore($conn, $semname, $identifier);

        return false;
    }

    return true;
}

function acquire_semaphore_with_lock($conn, $semname, $limit, $timeout = 10)
{
    $identifier = acquire_lock($conn, $semname, $acquire_timeout = .01);
    if ($identifier) {
        try {
            return acquire_fair_semaphore($conn, $semname, $limit, $timeout);
        } finally {
            release_lock($conn, $semname, $identifier);
        }
    }
}

function send_sold_email_via_queue($conn, $seller, $item, $price, $buyer)
{
    $data = [
        'seller_id' => $seller,
        'item_id'   => $item,
        'price'     => $price,
        'buyer_id'  => $buyer,
        'time'      => microtime(true),
    ];

    $conn->rpush('queue:email', json_encode($data));
}

function process_sold_email_queue($conn, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $packed = $conn->blpop(['queue:email'], 30);
        if (!$packed) {
            continue;
        }

        $to_send = json_decode($packed[1], true);
        $result = fetch_data_and_send_sold_email($to_send);
        if ($result === false) {
            log_error("Failed to send sold email");
        } else {
            log_success("Sent sold email");
        }
    }
}

function worker_watch_queue($conn, $queue, $callbacks, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $packed = $conn->blpop([$queue], 30);
        if (!$packed) {
            continue;
        }

        list($name, $args) = json_decode($packed[1], true);
        if (!array_key_exists($name, $callbacks)) {
            log_error(sprintf("Unknown callback %s", $name));
            continue;
        }

        call_user_func_array($callbacks[$name], $args);
    }
}

function worker_watch_queues($conn, $queues, $callbacks, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $packed = $conn->blpop($queues, 30);
        if (!$packed) {
            continue;
        }

        list($name, $args) = json_decode($packed[1], true);
        if (!array_key_exists($name, $callbacks)) {
            log_error(sprintf("Unknown callback %s", $name));
            continue;
        }

        call_user_func_array($callbacks[$name], $args);
    }
}

function fetch_data_and_send_sold_email($to_send)
{
    // simply return true
    return true;
}

function log_success($msg)
{
    // do nothing
}

function log_error($msg)
{
    // do nothing
}

function execute_later($conn, $queue, $name, $args, $delay = 0)
{
    $identifier = Uuid::uuid4()->toString();
    $item = json_encode([$identifier, $queue, $name, $args]);
    if ($delay > 0) {
        $conn->zadd('delayed:', [$item => microtime(true) + $delay]);
    } else {
        $conn->rpush('queue:' . $queue, $item);
    }

    return $identifier;
}

function poll_queue($conn, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $item = $conn->zrange('delayed:', 0, 0, ['WITHSCORES' => true]);
        if (!$item OR reset($item) > microtime(true)) {
            usleep(10000);

            continue;
        }

        $item = key($item);

        list($identifier, $queue, $function, $args) = json_decode($item, true);

        $locked = acquire_lock($conn, $identifier);
        if (!$locked) {
            continue;
        }

        if ($conn->zrem('delayed:', $item)) {
            $conn->rpush('queue:' . $queue, $item);
        }

        release_lock($conn, $identifier, $locked);
    }
}

function create_chat($conn, $sender, $recipients, $message, $chat_id = null)
{
    $chat_id = $chat_id ?: strval($conn->incr('ids:chat:'));

    $recipients[] = $sender;
    $recipientsd  = array_fill_keys($recipients, 0);

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zadd('chat:' . $chat_id, $recipientsd);
    foreach ($recipients as $rec) {
        $pipeline->zadd('seen:' . $rec, [$chat_id => 0]);
    }
    $pipeline->execute();

    return send_message($conn, $chat_id, $sender, $message);
}

function send_message($conn, $chat_id, $sender, $message)
{
    $identifiter = acquire_lock($conn, 'chat:' . $chat_id);
    if (!$identifiter) {
        throw new \Exception("Couldn't get the lock");
    }

    try {
        $mid = $conn->incr('ids:' . $chat_id);
        $ts  = microtime(true);
        $packed = json_encode([
            'id'      => $mid,
            'ts'      => $ts,
            'sender'  => $sender,
            'message' => $message
        ]);

        $conn->zadd('msgs:' . $chat_id, [$packed => $mid]);

    } finally {
        release_lock($conn, 'chat:' . $chat_id, $identifiter);
    }

    return $chat_id;
}

function fetch_pending_messages($conn, $recipient)
{
    $seen = $conn->zrange('seen:' . $recipient, 0 , -1, ['WITHSCORES' => true]);

    $pipeline = $conn->pipeline(['atomic' => true]);

    foreach ($seen as $chat_id => $seen_id) {
        $pipeline->zrangebyscore('msgs:' . $chat_id, $seen_id + 1, '+inf');
    }
    $results = $pipeline->execute();

    $chat_info = [];

    reset($seen);
    reset($results);
    while (list($chat_id, $seen_id) = each($seen)) {
        $messages = each($results)['value'];
        if (!$messages) {
            continue;
        }

        $messages = array_map(
            function ($message) { return json_decode($message, true); },
            $messages
        );

        $seen_id = end($messages)['id'];
        $conn->zadd('chat:' . $chat_id, [$recipient => $seen_id]);

        $min_id = $conn->zrange('chat:' . $chat_id, 0, 0, ['WITHSCORES' => true]);

        $pipeline->zadd('seen:' . $recipient, [$chat_id => $seen_id]);

        if ($min_id) {
            $pipeline->zremrangebyscore('msgs:' . $chat_id, 0, reset($min_id));
        }

        $chat_info[$chat_id] = $messages;
    }

    return $chat_info;
}

function join_chat($conn, $chat_id, $user)
{
    $message_id = intval($conn->get('ids:' . $chat_id));

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zadd('chat:' . $chat_id, [$user => $message_id]);
    $pipeline->zadd('seen:' . $user, [$chat_id => $message_id]);
    $pipeline->execute();
}

function leave_chat($conn, $chat_id, $user)
{
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zrem('chat:' . $chat_id, $user);
    $pipeline->zrem('seen:' . $user, $chat_id);
    $pipeline->zcard('chat:' . $chat_id);
    $results = $pipeline->execute();

    if (!end($results)) {
        $conn->del('msg:' . $chat_id, 'ids:' . $chat_id);
    } else {
        $oldest = $conn->zrange('chat:' . $chat_id, 0, 0, ['WITHSCORES' => true]);
        $conn->zremrangebyscore('msgs:' . $chat_id, 0, reset($oldest));
    }
}

global $AGGREGATES;

$AGGREGATES = [];

function daily_country_aggregates($conn, $line)
{
    global $AGGREGATES;

    if ($line) {
        $line    = explode(' ', $line);
        $ip      = $line[0];
        $day     = $line[1];
        $country = find_city_by_ip_local($ip)[2];

        $AGGREGATES[$day][$country] += 1;

        return;
    }

    foreach ($AGGREGATES as $day => $aggregate) {
        $conn->zadd('daily:country:' . $day, $aggregate);
        unset($AGGREGATES[$day]);
    }
}

function find_city_by_ip_local($ip)
{
    // simple mock
    return [ 'city-' . $ip, 'region-' . $ip, 'country-' . $ip];
}

function copy_logs_to_redis(
    $conn, $path, $channel, $count = 10, $limit = 2 ** 30, $quit_when_done = true
)
{
    $bytes_in_redis = 0;
    $waiting = [];

    create_chat(
        $conn, 'source', array_map('strval', range(0, $count - 1)), '', $channel
    );
    $count = strval($count);
    foreach (array_diff(scandir($path, SCANDIR_SORT_ASCENDING), ['..', '.']) as $logfile) {
        $full_path = rtrim($path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $logfile;

        $fsize = filesize($full_path);
        while ($bytes_in_redis + $fsize > $limit) {
            $cleaned = _clean($conn, $channel, $waiting, $count);
            if ($cleaned) {
                $bytes_in_redis -= $cleaned;
            } else {
                usleep(250000);
            }
        }

        $inp = fopen($full_path, 'rb');
        $block = ' ';
        while ($block) {
            $block = fread($inp, 2 ** 17);
            $conn->append($channel . $logfile, $block);
        }
        fclose($inp);

        send_message($conn, $channel, 'source', $logfile);

        $bytes_in_redis += $fsize;
        $waiting[] = [$logfile, $fsize];
    }

    if ($quit_when_done) {
        send_message($conn, $channel, 'source', ':done');
    }

    while ($waiting) {
        $cleaned = _clean($conn, $channel, $waiting, $count);
        if ($cleaned) {
            $bytes_in_redis -= $cleaned;
        } else {
            usleep(250000);
        }
    }
}

function _clean($conn, $channel, &$waiting, $count)
{
    if (!$waiting) {
        return 0;
    }
    
    $w0 = $waiting[0][0];
    if ($conn->get($channel . $w0 . ':done') == $count) {
        $conn->del($channel . $w0, $channel . $w0 . ':done');
        
        return array_shift($waiting)[1];
    }

    return 0;
}

function process_logs_from_redis($conn, $id, $callback)
{
    while (1) {
        $fdata = fetch_pending_messages($conn, $id);

        foreach ($fdata as $ch => $mdata) {
            foreach ($mdata as $message) {
                $logfile = $message['message'];

                if ($logfile == ':done') {
                    return;
                } elseif (!$logfile) {
                    continue;
                }

                $block_reader = __NAMESPACE__ . '\readblocks';
                if (substr($logfile, -strlen('.gz')) === '.gz') {
                    $block_reader = __NAMESPACE__ . '\readblocks_gz';
                }

                foreach (readlines($conn, $ch . $logfile, $block_reader) as $line) {
                    $callback($conn, $line);
                }
                $callback($conn, null);

                $conn->incr($ch . $logfile . ':done');
            }
        }

        if (!$fdata) {
            usleep(100000);
        }
    }
}

function readlines($conn, $key, $rblocks)
{
    $out = '';
    foreach ($rblocks($conn, $key) as $block) {
        $out .= $block;
        $posn = strrpos($out, "\n");
        if ($posn > 0) {
            foreach (explode("\n", substr($out, 0, $posn)) as $line) {
                yield $line . "\n";
            }
            $out = substr($out, $posn + 1);
        }

        if (!$block) {
            yield $out;

            break;
        }
    }
}

function readblocks($conn, $key, $blocksize = 2 ** 17)
{
    $lb  = $blocksize;
    $pos = 0;
    while ($lb == $blocksize) {
        $block = $conn->getrange($key, $pos, $pos + $blocksize - 1);
        yield $block;

        $lb = strlen($block);
        $pos += $lb;
    }
    yield '';
}

function readblocks_gz($conn, $key)
{
    // todo
//    $inp = '';
//    $decoder = null;
//    foreach (readblocks($conn, $key, 2 ** 17) as $block) {
//        if (!$decoder) {
//            $inp .= $block;
//
//            $exception_caught = false;
//            try {
//                if (substr($inp, 0, 3) != "\x1f\x8b\x08") {
//                    throw new \Exception("invalid gzip data");
//                }
//
//                $i = 10;
//                $flag = ord($inp[3]);
//                if ($flag & 4) {
//                    $i += 2 + ord($inp[$i]) + 256 * ord($inp[$i + 1]);
//                }
//                if ($flag & 8) {
//                    $i = strpos($inp, "\0", $i) + 1;
//                }
//                if ($flag & 16) {
//                    $i = strpos($inp, "\0", $i) + 1;
//                }
//                if ($flag & 2) {
//                    $i += 2;
//                }
//                if ($i > strlen($inp)) {
//                    throw new \OutOfRangeException("not enough data");
//                }
//            } catch (\OutOfRangeException $e) {
//                $exception_caught = true;
//
//                continue;
//            } catch (\InvalidArgumentException $e) {
//                $exception_caught = true;
//
//                continue;
//            }
//
//            if (!$exception_caught) {
//                $block = substr($inp, $i);
//                $inp = null;
//                $decoder = 'zlib_decode';
//                if (!$block) {
//                    continue;
//                }
//
//            }
//        }
//
//        if (!$block) {
//
//            break;
//        }
//
//        yield $decoder($block);
//    }
}