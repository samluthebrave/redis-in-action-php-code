<?php

/**
 * Note: opening a thread from inside a thread is problematic, thus opening all
 * at once instead. Use `execute_in_thread()` instead of `execute_later()`.
 */

namespace RedisInAction\Ch08;

use RedisInAction\Helper\Threading;

use function RedisInAction\Ch06\acquire_lock_with_timeout;
use function RedisInAction\Ch06\release_lock;

function execute_in_thread($conn, $func_name, $args)
{
    assert($conn === $args[0]);

    $t = new Threading(__NAMESPACE__ . '\\' . $func_name, $args);

    $t->start();
}

function create_user($conn, $login, $name)
{
    $llogin = strtolower($login);
    $lock = acquire_lock_with_timeout($conn, 'user:' . $llogin, 1);
    if (!$lock) {
        return null;
    }

    if ($conn->hget('users:', $llogin)) {
        release_lock($conn, 'user:' . $llogin, $lock);

        return null;
    }

    $id = $conn->incr('user:id:');
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->hset('users:', $llogin, $id);
    $pipeline->hmset(sprintf('user:%s', $id), [
        'login'     => $login,
        'id'        => $id,
        'name'      => $name,
        'followers' => 0,
        'following' => 0,
        'posts'     => 0,
        'signup'    => microtime(true),
    ]);
    $pipeline->execute();

    release_lock($conn, 'user:' . $llogin, $lock);

    return $id;
}

function create_status($conn, $uid, $message, $data = [])
{
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->hget(sprintf('user:%s', $uid), 'login');
    $pipeline->incr('status:id:');
    list($login, $id) = $pipeline->execute();

    if (!$login) {
        return null;
    }

    $data = array_merge($data, [
        'message' => $message,
        'posted'  => microtime(true),
        'id'      => $id,
        'uid'     => $uid,
        'login'   => $login
    ]);
    $pipeline->hmset(sprintf('status:%s', $id), $data);
    $pipeline->hincrby(sprintf('user:%s', $uid), 'posts', 1);
    $pipeline->execute();

    return $id;
}

function get_status_messages($conn, $uid, $timeline = 'home:', $page = 1, $count = 30)
{
    $statuses = $conn->zrevrange(
        sprintf('%s%s', $timeline, $uid), ($page - 1) * $count, $page * $count - 1
    );
    $pipeline = $conn->pipeline(['atomic' => true]);
    foreach ($statuses as $id) {
        $pipeline->hgetall(sprintf('status:%s', $id));
    }

    return array_filter($pipeline->execute());
}

global $HOME_TIMELINE_SIZE;

$HOME_TIMELINE_SIZE = 1000;

function follow_user($conn, $uid, $other_uid)
{
    global $HOME_TIMELINE_SIZE;

    $fkey1 = sprintf('following:%s', $uid);
    $fkey2 = sprintf('followers:%s', $other_uid);

    if ($conn->zscore($fkey1, $other_uid)) {
        return null;
    }

    $now = microtime(true);

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zadd($fkey1, [$other_uid => $now]);
    $pipeline->zadd($fkey2, [$uid => $now]);
    $pipeline->zrevrange(
        sprintf('profile:%s', $other_uid),
        0,
        $HOME_TIMELINE_SIZE - 1,
        ['WITHSCORES' => true]
    );

    list($following, $followers, $status_and_score) = array_slice(
        $pipeline->execute(), -3
    );

    $pipeline->hincrby(sprintf('user:%s', $uid), 'following', intval($following));
    $pipeline->hincrby(
        sprintf('user:%s', $other_uid), 'followers', intval($followers)
    );
    if ($status_and_score) {
        $pipeline->zadd(sprintf('home:%s', $uid), $status_and_score);
    }
    $pipeline->zremrangebyrank(sprintf('home:%s', $uid), 0, -$HOME_TIMELINE_SIZE - 1);
    $pipeline->execute();

    return true;
}

function unfollow_user($conn, $uid, $other_uid)
{
    global $HOME_TIMELINE_SIZE;

    $fkey1 = sprintf('following:%s', $uid);
    $fkey2 = sprintf('followers:%s', $other_uid);

    if (!$conn->zscore($fkey1, $other_uid)) {
        return null;
    }

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zrem($fkey1, $other_uid);
    $pipeline->zrem($fkey2, $uid);
    $pipeline->zrevrange(
        sprintf('profile:%s', $other_uid),
        0,
        $HOME_TIMELINE_SIZE - 1
    );
    list($following, $followers, $statuses) = array_slice(
        $pipeline->execute(), -3
    );

    $pipeline->hincrby(sprintf('user:%s', $uid), 'following', -intval($following));
    $pipeline->hincrby(
        sprintf('user:%s', $other_uid), 'followers', -intval($followers)
    );
    if ($statuses) {
        $pipeline->zrem(sprintf('home:%s', $uid), $statuses);
    }
    $pipeline->execute();

    return true;
}

global $REFILL_USERS_STEP;

$REFILL_USERS_STEP = 50;

function refill_timeline($conn, $incoming, $timeline)
{
    global $REFILL_USERS_STEP;

    if ($conn->zcard($timeline) >= 750) {
        return;
    }

    $users_count = $conn->zcard($incoming);
    $batches = ceil($users_count / $REFILL_USERS_STEP);
    for ($batch = 0; $batch < $batches; $batch++) {
        $start = $batch * $REFILL_USERS_STEP;
        $stop  = $start + $REFILL_USERS_STEP - 1;
        if ($batch == 0) {
            refill_timeline_in_range($conn, $incoming, $timeline, $start, $stop);
        } else {
            execute_in_thread(
                $conn,
                'refill_timeline_in_range',
                [$conn, $incoming, $timeline, $start, $stop]
            );
        }
    }
}

function refill_timeline_in_range($conn, $incoming, $timeline, $start, $stop)
{
    global $HOME_TIMELINE_SIZE;

    $users = $conn->zrange($incoming, $start, $stop);

    $pipeline = $conn->pipeline(['atomic' => false]);
    foreach ($users as $uid) {
        $pipeline->zrevrange(
            sprintf('profile:%s', $uid),
            0,
            $HOME_TIMELINE_SIZE - 1,
            ['WITHSCORES' => true]
        );
    }

    $messages = [];
    foreach ($pipeline->execute() as $results) {
        $messages += $results;
    }
    arsort($messages);
    $messages = array_slice($messages, 0, $HOME_TIMELINE_SIZE, true);

    $pipeline = $conn->pipeline(['atomic' => true]);
    if ($messages) {
        $pipeline->zadd($timeline, $messages);
    }
    $pipeline->zremrangebyrank($timeline, 0, -$HOME_TIMELINE_SIZE - 1);
    $pipeline->execute();
}

function follow_user_list($conn, $other_uid, $list_id)
{
    global $HOME_TIMELINE_SIZE;

    $fkey1 = sprintf('list:in:%s', $list_id);
    $fkey2 = sprintf('list:out:%s', $other_uid);
    $timeline = sprintf('list:statuses:%s', $list_id);

    if ($conn->zscore($fkey1, $other_uid)) {
        return null;
    }

    $now = microtime(true);

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zadd($fkey1, [$other_uid => $now]);
    $pipeline->zadd($fkey2, [$list_id => $now]);
    $pipeline->zrevrange(
        sprintf('profile:%s', $other_uid),
        0,
        $HOME_TIMELINE_SIZE - 1,
        ['WITHSCORES' => true]
    );
    list($following, $followers, $status_and_score) = array_slice(
        $pipeline->execute(), -2
    );
    $pipeline->hincrby(
        sprintf('list:%s', $list_id), 'following', intval($following)
    );

    $pipeline->zadd($timeline, $status_and_score);
    $pipeline->zremrangbyrank($timeline, 0, -$HOME_TIMELINE_SIZE - 1);
    $pipeline->execute();

    return true;
}

function unfollow_user_list($conn, $other_uid, $list_id)
{
    global $HOME_TIMELINE_SIZE;

    $fkey1 = sprintf('list:in:%s', $list_id);
    $fkey2 = sprintf('list:out:%s', $other_uid);
    $timeline = sprintf('list:statuses:%s', $list_id);

    if (!$conn->zscore($fkey1, $other_uid)) {
        return null;
    }

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zrem($fkey1, $other_uid);
    $pipeline->zrem($fkey2, $list_id);
    $pipeline->zrevrange(
        sprintf('profile:%s', $other_uid),
        0,
        $HOME_TIMELINE_SIZE - 1
    );
    list($following, $followers, $statuses) = array_slice(
        $pipeline->execute(), -2
    );
    $pipeline->hincrby(
        sprintf('list:%s', $list_id), 'following', -intval($following)
    );

    if ($statuses) {
        $pipeline->zrem($timeline, $statuses);
        refill_timeline($conn, $fkey1, $timeline);
    }

    $pipeline->execute();

    return true;
}

function create_user_list($conn, $uid, $name)
{
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->hget(sprintf('users:%s', $uid), 'login');
    $pipeline->incr('list:id:');
    list($login, $id) = $pipeline->execute();

    if (!$login) {
        return null;
    }

    $now = microtime(true);

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zadd(sprintf('lists:%s', $uid), [$id => $now]);
    $pipeline->hmset(sprintf('list:%s', $id), [
        'name'      => $name,
        'id'        => $id,
        'uid'       => $uid,
        'login'     => $login,
        'following' => 0,
        'created'   => $now,
    ]);
    $pipeline->execute();

    return $id;
}

function post_status($conn, $uid, $message, $data = [])
{
    $id = create_status($conn, $uid, $message, $data);
    if (!$id) {
        return null;
    }

    $posted = $conn->hget(sprintf('status:%s', $id), 'posted');
    if (!$posted) {
        return null;
    }

    $post = [strval($id) => floatval($posted)];
    $conn->zadd(sprintf('profile:%s', $uid), $post);

    syndicate_status($conn, $uid, $post);

    return $id;
}

global $POSTS_PER_PASS;

$POSTS_PER_PASS = 1000;

function syndicate_status($conn, $uid, $post)
{
    global $POSTS_PER_PASS;

    $followers_count = $conn->zcard(sprintf('followers:%s', $uid));
    $batches = ceil($followers_count / $POSTS_PER_PASS);
    for ($batch = 0; $batch < $batches; $batch++) {
        $start = $batch * $POSTS_PER_PASS;
        $stop  = $start + $POSTS_PER_PASS - 1;
        if ($batch == 0) {
            syndicate_status_in_range($conn, $uid, $post, $start, $stop);
        } else {
            execute_in_thread(
                $conn,
                'syndicate_status_in_range',
                [$conn, $uid, $post, $start, $stop]
            );
        }
    }
}

function syndicate_status_in_range($conn, $uid, $post, $start, $stop)
{
    global $HOME_TIMELINE_SIZE;

    $followers = $conn->zrange(sprintf('followers:%s', $uid), $start, $stop);
    $pipeline = $conn->pipeline(['atomic' => false]);
    foreach ($followers as $follower) {
        $pipeline->zadd(sprintf('home:%s', $follower), $post);
        $pipeline->zremrangebyrank(
            sprintf('home:%s', $follower), 0, -$HOME_TIMELINE_SIZE - 1
        );
    }
    $pipeline->execute();
}

function delete_status($conn, $uid, $status_id)
{
    $key  = sprintf('status:%s', $status_id);
    $lock = acquire_lock_with_timeout($conn, $key, 1);
    if (!$lock) {
        return null;
    }

    if ($conn->hget($key, 'uid') != strval($uid)) {
        release_lock($conn, $key, $lock);

        return null;
    }

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->del($key);
    $pipeline->zrem(sprintf('profile:%s', $uid), $status_id);
    $pipeline->zrem(sprintf('home:%s', $uid), $status_id);
    $pipeline->hincrby(sprintf('user:%s', $uid), 'posts', -1);
    $pipeline->execute();

    release_lock($conn, $key, $lock);

    return true;
}

function clean_timelines($conn, $uid, $status_id)
{
    global $POSTS_PER_PASS;

    $to_clean = [
        [
            sprintf('followers:%s', $uid),
            'home:%s'
        ],
        [
            sprintf('list:out:%s', $uid),
            'list:statuses:%s'
        ]
    ];

    foreach ($to_clean as list($key, $base)) {
        $followers_count = $conn->zcard($key);
        $batches = ceil($followers_count / $POSTS_PER_PASS);
        for ($batch = 0; $batch < $batches; $batch++) {
            $start = $batch * $POSTS_PER_PASS;
            $stop  = $start + $POSTS_PER_PASS - 1;
            if ($batch == 0) {
                clean_timelines_in_range(
                    $conn, $key, $base, $status_id, $start, $stop
                );
            } else {
                execute_in_thread(
                    $conn,
                    'clean_timelines_in_range',
                    [$conn, $key, $base, $status_id, $start, $stop]
                );
            }
        }
    }
}

function clean_timelines_in_range($conn, $key, $base, $status_id, $start, $stop)
{
    $followers = $conn->zrange($key, $start, $stop);
    $pipeline = $conn->pipeline(['atomic' => false]);
    foreach ($followers as $follower) {
        $pipeline->zrem(sprintf($base, $follower), $status_id);
    }
    $pipeline->execute();
}