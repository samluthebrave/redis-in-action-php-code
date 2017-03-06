<?php

namespace RedisInAction\Ch06;

use Ramsey\Uuid\Uuid;
use Predis\PredisException;
use Predis\Transaction\AbortedMultiExecException;

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
    $identifier = Uuid::uuid4()->toString();

    $end = microtime(true) + $acquire_timeout;
    while (microtime(true) < $end) {
        if ($conn->setnx('lock:' . $lockname, $identifier)) {
            return $identifier;
        }

        usleep(1);
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

    } catch (PredisException $e) {
        // do nothing
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

        usleep(1);
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
        } catch (PredisException $e) {
            // do nothing
        } finally {
            release_lock($conn, $semname, $identifier);
        }
    }
}