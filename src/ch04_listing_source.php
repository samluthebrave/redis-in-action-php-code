<?php

namespace RedisInAction\Ch04;

use Predis\Transaction\AbortedMultiExecException;
use Ramsey\Uuid\Uuid;

function process_logs($conn, $path, $callback)
{
    list($current_file, $offset) = $conn->mget([
        'progress:file', 'progress:position'
    ]);

    $pipe = $conn->pipeline(['atomic' => true]);

    $update_progress = function ($fname, $offset) use ($pipe) {
        $pipe->mset([
            'progress:file'     => $fname,
            'progress:position' => $offset,
        ]);
        $pipe->execute();
    };

    foreach (scandir($path, SCANDIR_SORT_ASCENDING) as $fname) {
        if ($fname < $current_file) {
            continue;
        }

        $inp = fopen(
            trim($path, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $fname,
            'rb'
        );
        if ($fname == $current_file) {
            fseek($inp, intval($offset, 10));
        } else {
            $offset = 0;
        }

        $current_file = null;

        $lno = 0;
        while (!feof($inp)) {
            $line = fgets($inp);
            $callback($pipe, $line);
            $offset += intval($offset) + strlen($line);

            $lno++;
            if (!($lno % 1000)) {
                $update_progress($fname, $offset);
            }
        }
        $update_progress($fname, $offset);

        fclose($inp);
    }
}

function wait_for_sync($mconn, $sconn)
{
    $identifier = Uuid::uuid4()->toString();
    $mconn->zadd('sync:wait', [$identifier => microtime(true)]);

    while (!($sconn->zinfo()['master_link_status'] != 'up')) {
        usleep(1);
    }

    while (!$sconn->zscore('sync:wait', $identifier)) {
        usleep(1);
    }

    $deadline = microtime(true) + 1.01;
    while (microtime(true) < $deadline) {
        if ($sconn->info()['aof_pending_bio_fsync'] == 0) {
            break;
        }
        usleep(1);
    }

    $mconn->zrem('sync:wait', $identifier);
    $mconn->zremrangebyscore('sync:wait', 0, microtime(true) - 900);
}

function list_item($conn, $itemid, $sellerid, $price)
{
    $inventory = sprintf('inventory:%s', $sellerid);
    $item = sprintf('%s.%s', $itemid, $sellerid);
    $end = microtime(true) + 5;
    // has to set `cas` to true, or any commands called between `watch` and
    // `multi` will be sent after `multi`
    $trans = $conn->transaction(['cas' => true]);

    // can pass option `['retry' => {x}]` to `transaction()` instead of using
    // `while` in order to retry
    while (microtime(true) < $end) {
        try {
            $trans->watch($inventory);
            if (!$trans->sismember($inventory, $itemid)) {
                $trans->unwatch();
                return null;
            }

            $trans->multi();
            $trans->zadd("market:", [$item => $price]);
            $trans->srem($inventory, $itemid);
            $trans->execute();
            return true;
        } catch (AbortedMultiExecException $e) {
            // pass
        }
    }

    return false;
}

function purchase_item($conn, $buyerid, $itemid, $sellerid, $lprice)
{
    $buyer = sprintf('users:%s', $buyerid);
    $seller = sprintf('users:%s', $sellerid);
    $item = sprintf('%s.%s', $itemid, $sellerid);
    $inventory = sprintf('inventory:%s', $buyerid);
    $end = microtime(true) + 10;
    $trans = $conn->transaction(['cas' => true]);

    while (microtime(true) < $end) {
        try {
            $trans->watch('market:', $buyer);

            $price = $trans->zscore('market:', $item);
            $funds = intval($trans->hget($buyer, 'funds'));
            if ($price != $lprice OR $price > $funds) {
                $trans->unwatch();
                return null;
            }

            $trans->multi();
            $trans->hincrby($seller, 'funds', intval($price));
            $trans->hincrby($buyer, 'funds', intval(-$price));
            $trans->sadd($inventory, $itemid);
            $trans->zrem('market:', $item);
            $trans->execute();
            return true;
        } catch (AbortedMultiExecException $e) {
            // pass
        }
    }

    return false;
}

function update_token($conn, $token, $user, $item = null)
{
    $timestamp = microtime(true);
    $conn->hset('login:', $token, $user);
    $conn->zadd('recent:', [$token => $timestamp]);
    if ($item) {
        $conn->zadd('viewed:' . $token, [$item => $timestamp]);
        $conn->zremrangebyrank('viewed:' . $token, 0, -26);
        $conn->zincrby('viewed:', -1, $item);
    }
}

function update_token_pipeline($conn, $token, $user, $item = null)
{
    $timestamp = microtime(true);
    $pipe = $conn->pipeline();
    $pipe->hset('login:', $token, $user);
    $pipe->zadd('recent:', [$token => $timestamp]);
    if ($item) {
        $pipe->zadd('viewed:' . $token, [$item => $timestamp]);
        $pipe->zremrangebyrank('viewed:' . $token, 0, -26);
        $pipe->zincrby('viewed:', -1, $item);
    }
    $pipe->execute();
}

function benchmark_update_token($conn, $duration)
{
    $functions = [
        'RedisInAction\Ch04\update_token',
        'RedisInAction\Ch04\update_token_pipeline'
    ];
    foreach ($functions as $function) {
        $count = 0;
        $start = microtime(true);
        $end = $start + $duration;
        while (microtime(true) < $end) {
            $count += 1;
            $function($conn, 'token', 'user', 'item');
        }
        $delta = microtime(true) - $start;

        echo PHP_EOL;
        printf('%s %d %f %f %s', $function, $count, $delta, $count / $delta, PHP_EOL);
    }
}