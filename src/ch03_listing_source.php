<?php

namespace RedisInAction\Ch03;

use RedisInAction\Helper\Threading;
use Predis\PredisException;

const ONE_WEEK_IN_SECONDS = 7 * 86400;
const VOTE_SCORE = 432;
const ARTICLES_PER_PAGE = 25;

function update_token($conn, $token, $user, $item = null)
{
    $timestamp = microtime(true);
    $conn->hset('login:', $token, $user);
    $conn->zadd('recent:', [$token => $timestamp]);
    if ($item) {
        $key = 'viewed:' . $token;
        $conn->lrem($key, $item);
        $conn->rpush($key, $item);
        $conn->ltrim($key, -25, -1);
    }
    $conn->zincrby('viewed:', -1, $item);
}

function publisher($conn, $n)
{
    sleep(1);
    for ($i = 0; $i < $n; $i++) {
        $conn->publish('channel', $i);
        sleep(1);
    }
}

function run_pubsub($conn)
{
    (new Threading('RedisInAction\Ch03\publisher', [$conn, 3]))->start();
    $pubsub = $conn->pubsub();
    $pubsub->subscribe('channel');
    $count = 0;
    foreach ($pubsub->listen() as $item) {
        print_r($item);
        $count += 1;
        if ($count == 4) {
            $pubsub->unsubsribe();
        }
        if ($count == 5) {
            break;
        }
    }
}

function article_vote($conn, $user, $article)
{
    $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
    $posted = $conn->zscore('time:', $article);
    if ($posted < $cutoff) {
        return;
    }

    $partition = explode(':', $article);
    $article_id = end($partition);
    $pipeline = $conn->pipeline();
    $pipeline->sadd('voted:' . $article_id, $user);
    $pipeline->expire('voted:' . $article_id, intval($posted - $cutoff));
    if ($pipeline->excecute()[0]) {
        $pipeline->zincrby('score:', VOTE_SCORE, $article);
        $pipeline->hincrby($article, 'votes', 1);
        $pipeline->execute();
    }
}

function artivle_vote_v2($conn, $user, $article)
{
    $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
    $posted = $conn->zscore('time:', $article);
    $partition = explode(':', $article);
    $article_id = end($partition);
    $voted = 'voted:' . $article_id;

    $pipeline = $conn->pipeline();
    while ($posted > $cutoff) {
        try {
            $pipeline->watch($voted);
            if (!$pipeline->sismember($voted, $user)) {
                $pipeline->multi();
                $pipeline->sadd($voted, $user);
                $pipeline->expire($voted, intval($posted - $cutoff));
                $pipeline->zincrby('score:', VOTE_SCORE, $article);
                $pipeline->hincrby($article, 'votes', 1);
                $pipeline->execute();
            } else {
                $pipeline->unwatch();
            }

        } catch (PredisException $e) {
            $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
        }
    }
}

function get_articles($conn, $page, $order = 'score:')
{
    $start = max($page - 1, 0) * ARTICLES_PER_PAGE;
    $end = $start + ARTICLES_PER_PAGE - 1;

    $ids = $conn->zrevrangebyscore($order, $start, $end);

    $pipeline = $conn->pipeline();
    array_map(
        function ($id) use ($pipeline) {
            $pipeline->hgetall($id);
        },
        $ids
    );

    $articles = [];
    foreach (array_combine($ids, $pipeline->execute()) as $id => $article_data) {
        $article_data['id'] = $id;
        $articles[] = $article_data;
    }

    return $articles;
}

const THIRTY_DAYS = 30 * 86400;

function check_token($conn, $token) {
    return $conn->get('login:' + $token);
}

function update_token_v2($conn, $token, $user, $item = null)
{
    $conn->setex('login:' . $token, $user, THIRTY_DAYS);
    $key = 'viewed:' . $token;
    if ($item) {
        $conn->lrem($key, $item);
        $conn->rpush($key, $item);
        $conn->ltrim($key, -25, -1);
    }
    $conn->expire($key, THIRTY_DAYS);
    $conn->zincrby('viewed:', -1, $item);
}

function add_to_cart($conn, $session, $item, $count)
{
    $key = 'cart:' . $session;
    if ($count <= 0) {
        $conn->hrem($key, $item);
    } else {
        $conn->hset($key, $item, $count);
    }
    $conn->expire($key, THIRTY_DAYS);
}