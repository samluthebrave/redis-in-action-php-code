<?php

const ONE_WEEK_IN_SECONDS = 7 * 86400;
const VOTE_SCORE = 432;

function article_vote($conn, $user, $article)
{
    $cutoff = microtime(true) - ONE_WEEK_IN_SECONDS;
    if ($conn->zscore('time:', $article) < $cutoff) {
        return;
    }

    $partition = explode(':', $article);
    $article_id = end($partition);
    if ($conn->sadd('voted:' . $article_id, $user)) {
        $conn->zincrby('score:', VOTE_SCORE, $article);
        $conn->hincrby($article, 'votes', 1);
    }
}

function post_article($conn, $user, $title, $link)
{
    $article_id = strval($conn->incr('article:'));

    $voted = 'voted:' . $article_id;
    $conn->sadd($voted, $user);
    $conn->expire($voted, ONE_WEEK_IN_SECONDS);

    $now = microtime(true);
    $article = 'article:' . $article_id;
    $conn->hmset($article, [
        'title'  => $title,
        'link'   => $link,
        'poster' => $user,
        'time'   => $now,
        'votes'  => 1
    ]);

    $conn->zadd('score:', [$article => $now + VOTE_SCORE]);
    $conn->zadd('time:', [$article => $now]);

    return $article_id;
}

const ARTICLES_PER_PAGE = 25;

function get_articles($conn, $page, $order = 'score:')
{
    $start = ($page - 1) * ARTICLES_PER_PAGE;
    $end = $start + ARTICLES_PER_PAGE - 1;

    $ids = $conn->zrevrange($order, $start, $end);
    $articles = [];
    foreach ($ids as $id) {
        $article_data = $conn->hgetall($id);
        $article_data['id'] = $id;
        $articles[] = $article_data;
    }

    return $articles;
}

function add_remove_groups($conn, $article_id, $to_add = [], $to_remove = [])
{
    $article = 'article:' . $article_id;
    foreach ($to_add as $group) {
        $conn->sadd('group:' . $group, $article);
    }
    foreach ($to_remove as $group) {
        $conn->srem('group:' . $group, $article);
    }
}

function get_group_articles($conn, $group, $page, $order = 'score:')
{
    $key = $order . $group;
    if (!$conn->exists($key)) {
        $conn->zinterstore($key,
            ['group:' . $group, $order],
            ['AGGREGATE' => 'max']
        );
        $conn->expire($key, 60);
    }
    return get_articles($conn, $page, $key);
}
