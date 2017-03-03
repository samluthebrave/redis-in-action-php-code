<?php

namespace RedisInAction\Ch06;

use Ramsey\Uuid\Uuid;
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