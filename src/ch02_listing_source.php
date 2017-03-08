<?php

namespace RedisInAction\Ch02;

use RedisInAction\Helper\Threading;

function check_token($conn, $token)
{
    return $conn->hget('login:', $token);
}

function update_token($conn, $token, $user, $item = null)
{
    $timestamp = microtime(true);
    $conn->hset('login:', $token, $user);
    $conn->zadd('recent:', [$token => $timestamp]);
    if ($item) {
        $conn->zadd('viewed:' . $token, [$item => $timestamp]);
        $conn->zremrangebyrank('viewed:' . $token, 0, -26);
        $conn->zincrby('viewed:', -1, $item); // later added
    }
}

global $QUIT, $LIMIT;
$QUIT = false;
$LIMIT = 10000000;

function clean_sessions($conn, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $size = $conn->zcard('recent:');
        if ($size <= $thread->getGlobal('LIMIT')) {
            sleep(1);
            continue;
        }

        $end_index = min($size - $thread->getGlobal('LIMIT'), 100);
        $tokens = $conn->zrange('recent:', 0, $end_index - 1);

        $session_keys = [];
        foreach ($tokens as $token) {
            $session_keys[] = 'viewed:' . $token;
        }

        $conn->del($session_keys);
        $conn->hdel('login:', $tokens);
        $conn->zrem('recent:', $tokens);
    }
}

function add_to_cart($conn, $session, $item, $count)
{
    if ($count <= 0) {
        $conn->hrem('cart:' . $session, $item);
    } else {
        $conn->hset('cart:' . $session, $item, $count);
    }
}

function clean_full_sessions($conn, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $size = $conn->zcard('recent:');
        if ($size <= $thread->getGlobal('LIMIT')) {
            sleep(1);
            continue;
        }

        $end_index = min($size - $thread->getGlobal('LIMIT'), 100);
        $tokens = $conn->zrange('recent:', 0, $end_index - 1);

        $session_keys = [];
        foreach ($tokens as $token) {
            $session_keys[] = 'viewed:' . $token;
            $session_keys[] = 'cart:' . $token;
        }

        $conn->del($session_keys);
        $conn->hdel('login:', $tokens);
        $conn->zrem('recent:', $tokens);
    }
}

function cache_request($conn, $request, $callback)
{
    if (!can_cache($conn, $request)) {
        return $callback($request);
    }

    $page_key = 'cache:' . hash_request($request);
    $content = $conn->get($page_key);

    if (!$content) {
        $content = $callback($request);
        $conn->setex($page_key, 300, $content);
    }

    return $content;
}

function schedule_row_cache($conn, $row_id, $delay)
{
    $conn->zadd('delay:', [$row_id => $delay]);
    $conn->zadd('schedule:', [$row_id => microtime(true)]);
}

function cache_rows($conn, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $next = $conn->zrange('schedule:', 0, 0, ['WITHSCORES' => true]);

        $now = microtime(true);
        if (!$next OR reset($next) > $now) {
            usleep(50000);

            continue;
        }

        $row_id = key($next);
        $delay = $conn->zscore('delay:', $row_id);
        if ($delay <= 0) {
            $conn->zrem('delay:', $row_id);
            $conn->zrem('schedule:', $row_id);
            $conn->del('inv:' . $row_id);
            continue;
        }

        $row = Inventory::get($row_id);
        $conn->zadd('schedule:', [$row_id => $now + $delay]);
        $conn->set('inv:' . $row_id, json_encode($row->to_dict()));
    }
}

// see above
// function update_token() { ... }

function rescale_viewed($conn, Threading $thread)
{
    while (!$thread->getGlobal('QUIT')) {
        $conn->zremrangebyrank('viewed', 0, -20001);
        $conn->zinterstore('viewed:', ['viewed:'], ['WEIGHTS' => [0.5]]);
        sleep(300);
    }
}

function can_cache($conn, $request)
{
    $item_id = extract_item_id($request);
    if (!$item_id OR is_dynamic($request)) {
        return false;
    }
    $rank = $conn->zrank('viewed:', $item_id);
    return $rank !== null AND $rank < 10000;
}

function extract_item_id($request)
{
    $query = parse_url($request, PHP_URL_QUERY);
    parse_str($query, $query);

    return isset($query['item']) ? $query['item'] : null;
}

function is_dynamic($request)
{
    $query = parse_url($request, PHP_URL_QUERY);
    parse_str($query, $query);

    return array_key_exists('_', $query);
}

function hash_request($request)
{
    return hash('md5', $request);
}

class Inventory
{
    private $id;

    public function __construct($id)
    {
        $this->id = $id;
    }

    public static function get($id)
    {
        return new self($id);
    }

    public function to_dict()
    {
        return [
            'id'     => $this->id,
            'data'   => 'data to cache...',
            'cached' => microtime(true)
        ];
    }
}