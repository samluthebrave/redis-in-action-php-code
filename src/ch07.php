<?php

namespace RedisInAction\Ch07;

use Ramsey\Uuid\Uuid;

const STOP_WORDS = 'able about across after all almost also am among
    an and any are as at be because been but by can cannot could dear did
    do does either else ever every for from get got had has have he her
    hers him his how however if in into is it its just least let like
    likely may me might most must my neither no nor not of off often on
    only or other our own rather said say says she should since so some
    than that the their them then there these they this tis to too twas us
    wants was we were what when where which while who whom why will with
    would yet you your';

const WORDS_RE = "[a-z']{2,}";

function tokenize($content)
{
    $words = [];
    preg_match_all('/' . WORDS_RE . '/i', strtolower($content), $matches);
    foreach ($matches[0] as $match) {
        $word = trim($match, "'");
        if (strlen($word) >= 2) {
            $words[] = $word;
        }
    }
    $stop_words = preg_split('/\s+/', STOP_WORDS);

    return array_diff($words, $stop_words);
}

function index_document($conn, $docid, $content)
{
    $words = tokenize($content);

    $pipeline = $conn->pipeline(['atomic' => true]);
    foreach ($words as $word) {
        $pipeline->sadd('idx:' . $word, $docid);
    }

    return count($pipeline->execute());
}

function _set_common($conn, $method, array $names, $ttl = 30, $execute = true)
{
    $id = Uuid::uuid4()->toString();
    $pipeline = $execute ? $conn->pipeline(['atomic' => true]) : $conn;
    $names = array_map(
        function ($name) { return 'idx:' . $name; },
        $names
    );
    call_user_func([$pipeline, $method], 'idx:' . $id, $names);
    $pipeline->expire('idx:' . $id, $ttl);
    if ($execute) {
        $pipeline->execute();
    }

    return $id;
}

function intersect($conn, $items, $ttl = 30, $_execute = true)
{
    return _set_common($conn, 'sinterstore', $items, $ttl, $_execute);
}

function union($conn, $items, $ttl = 30, $_execute = true)
{
    return _set_common($conn, 'sunionstore', $items, $ttl, $_execute);
}

function difference($conn, $items, $ttl = 30, $_execute = true)
{
    return _set_common($conn, 'sdiffstore', $items, $ttl, $_execute);
}

const QUERY_RE = "[+-]?[a-z']{2,}";

function parse($query)
{
    $unwanted = [];
    $all      = [];
    $current  = [];

    preg_match_all('/' . QUERY_RE . '/i', strtolower($query), $matches);
    foreach ($matches[0] as $match) {
        $word   = $match;
        $prefix = substr($word, 0, 1);
        if (in_array($prefix, array('+', '-'))) {
            $word = substr($word, 1);
        } else {
            $prefix = null;
        }
        $word = trim($word, "'");
        if (strlen($word) < 2 OR in_array($word, preg_split('/\s+/', STOP_WORDS))) {
            continue;
        }

        if ($prefix == '-') {
            $unwanted[] = $word;

            continue;
        }

        if ($current AND !$prefix) {
            $all[]   = array_values($current);
            $current = [];
        }
        $current[] = $word;
    }

    if ($current) {
        $all[] = array_values($current);
    }

    return [$all, array_values($unwanted)];
}

function parse_and_search($conn, $query, $ttl = 30)
{
    list($all, $unwanted) = parse($query);
    if (!$all) {
        return null;
    }

    $to_intersect = [];
    foreach ($all as $syn) {
        if (count($syn) > 1) {
            $to_intersect[] = union($conn, $syn, $ttl);
        } else {
            $to_intersect[] = $syn[0];
        }
    }
    if (count($to_intersect) > 1) {
        $intersect_result = intersect($conn, $to_intersect, $ttl);
    } else {
        $intersect_result = $to_intersect[0];
    }
    if ($unwanted) {
        array_unshift($unwanted, $intersect_result);

        return difference($conn, $unwanted, $ttl);
    }

    return $intersect_result;
}

function search_and_sort(
    $conn, $query, $id = null, $ttl = 300, $sort = '-updated', $start = 0, $num = 20
)
{
    $desc = strpos($sort, '-') === 0;
    $sort = ltrim($sort, '-');
    $by = "kb:doc:*->" . $sort;
    $alpha = !in_array($sort, ['updated', 'id', 'created']);

    if ($id AND !$conn->expire($id, $ttl)) {
        $id = null;
    }
    if (!$id) {
        $id = parse_and_search($conn, $query, $ttl);
    }
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->scard('idx:' . $id);
    $options = [
        'BY'    => $by,
        'LIMIT' => [$start, $num],
        'SORT'  => $desc ? 'DESC' : 'ASC',
        'ALPHA' => $alpha,
    ];
    $pipeline->sort('idx:' . $id, $options);
    $results = $pipeline->execute();

    return [$results[0], $results[1], $id];
}