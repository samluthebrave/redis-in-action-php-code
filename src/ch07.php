<?php

namespace RedisInAction\Ch07;

use Ramsey\Uuid\Uuid;

global $STOP_WORDS;

$STOP_WORDS = [
    'able','about','across','after','all','almost','also','am','among','an',
    'and','any','are','as','at','be','because','been','but','by','can',
    'cannot','could','dear','did','do','does','either','else','ever','every',
    'for','from','get','got','had','has','have','he','her','hers','him','his',
    'how','however','if','in','into','is','it','its','just','least','let',
    'like','likely','may','me','might','most','must','my','neither','no','nor',
    'not','of','off','often','on','only','or','other','our','own','rather',
    'said','say','says','she','should','since','so','some','than','that','the',
    'their','them','then','there','these','they','this','tis','to','too',
    'twas','us','wants','was','we','were','what','when','where','which',
    'while','who','whom','why','will','with','would','yet','you','your'
];

const WORDS_RE = "[a-z']{2,}";

function tokenize($content)
{
    global $STOP_WORDS;

    $words = [];
    preg_match_all('/' . WORDS_RE . '/i', strtolower($content), $matches);
    foreach ($matches[0] as $match) {
        $word = trim($match, "'");
        if (strlen($word) >= 2) {
            $words[] = $word;
        }
    }

    return array_diff($words, $STOP_WORDS);
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
    global $STOP_WORDS;

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
        if (strlen($word) < 2 OR in_array($word, $STOP_WORDS)) {
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

function search_and_zsort(
    $conn, $query, $id = null, $ttl = 300, $update = 1, $vote = 0, $start = 0, $num = 20, $desc = true
)
{
    if ($id AND !$conn->expire($id, $ttl)) {
        $id = null;
    }
    if (!$id) {
        $id = parse_and_search($conn, $query, $ttl);
        $scored_search = [
            $id           => 0,
            'sort:update' => $update,
            'sort:votes'  => $vote
        ];
        $id = zintersect($conn, $scored_search, $ttl);
    }
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->zcard('idx:' . $id);
    if ($desc) {
        $pipeline->zrevrange('idx:' . $id, $start, $start + $num - 1);
    } else {
        $pipeline->zrange('idx:' . $id, $start, $start + $num - 1);
    }
    $results = $pipeline->execute();

    return [$results[0], $results[1], $id];
}

function _zset_common($conn, $method, $scores, $ttl = 30, array $kw = array())
{
    $id = Uuid::uuid4()->toString();
    $execute = isset($kw['_execute']) ? $kw['_execute'] : true;
    unset($kw['_execute']);

    $pipeline = $execute ? $conn->pipeline() : $conn;
    foreach ($scores as $key => $score) {
        $scores['idx:' . $key] = $score;
        unset($scores[$key]);
    }

    $options = [];
    $options['WEIGHTS'] = array_values($scores);
    $options = array_merge($options, $kw);
    call_user_func(array($pipeline, $method), 'idx:' . $id, array_keys($scores), $options);
    $pipeline->expire('idx:' . $id, $ttl);
    if ($execute) {
        $pipeline->execute();
    }

    return $id;
}

function zunion($conn, $items, $ttl = 30, array $kw = array())
{
    return _zset_common($conn, 'zunionstore', $items, $ttl, $kw);
}

function zintersect($conn, $items, $ttl = 30, array $kw = array())
{
    return _zset_common($conn, 'zinterstore', $items, $ttl, $kw);
}

function string_to_score($string, $ignore_case = false)
{
    if ($ignore_case) {
        $string = strtolower($string);
    }
    $pieces = array_map('ord', str_split(substr($string, 0, 6)));
    while (count($pieces) < 6) {
        array_push($pieces, -1);
    }
    $score = 0;
    foreach ($pieces as $piece) {
        $score = $score * 257 + $piece + 1;
    }

    return $score * 2 + (strlen($string) > 6);
}

function to_char_map($set)
{
    $out = [];

    sort($set);
    foreach ($set as $pos => $val) {
        $out[$val] = $pos - 1;
    }

    return $out;
}

global $LOWER, $ALPHA, $LOWER_NUMERIC, $ALPHA_NUMERIC;

$LOWER = to_char_map(array_merge(range(ord('a'), ord('z')), [-1]));
$ALPHA = to_char_map(array_merge(range(ord('A'), ord('Z')), $LOWER));
$LOWER_NUMERIC = to_char_map(array_merge(range(ord('0'), ord('9')), $LOWER));
$ALPHA_NUMERIC = to_char_map(array_merge($ALPHA, $LOWER_NUMERIC));

function string_to_score_generic($string, $mapping)
{
    $length = intval(52 / log(count($mapping), 2));

    $pieces = array_map('ord', str_split(substr($string, 0, $length)));
    while (count($pieces) < $length) {
        array_push($pieces, -1);
    }

    $score = 0;
    foreach ($pieces as $piece) {
        $value = $mapping[$piece];
        $score = $score * count($mapping) + $value + 1;
    }

    return $score * 2 + (strlen($string) > $length);
}

function zadd_string($conn, $name, $kwargs)
{
    $pieces = [];
    foreach ($kwargs as $k => $v) {
        $pieces[$k] = string_to_score($v);
    }

    return $conn->zadd($name, $pieces);
}

function cpc_to_ecpm($views, $clicks, $cpc)
{
    return 1000. * $cpc * $clicks / $views;
}

function cpa_to_ecpm($views, $actions, $cpa)
{
    return 1000. * $cpa * $actions / $views;
}

function to_ecpm($type, $args)
{
    $func_map = [
        'cpc' => __NAMESPACE__ . '\cpc_to_ecpm',
        'cpa' => __NAMESPACE__ . 'cpa_to_ecpm',
        'cpm' => function () { return func_get_arg(0); }
    ];

    return call_user_func_array($func_map[$type], $args);
}

global $AVERAGE_PER_1K;

$AVERAGE_PER_1K = [];

function index_ad($conn, $id, $locations, $content, $type, $value)
{
    global $AVERAGE_PER_1K;

    $pipeline = $conn->pipeline(['atomic' => true]);

    foreach ($locations as $location) {
        $pipeline->sadd('idx:req:' . $location, $id);
    }

    $words = tokenize($content);
    foreach ($words as $word) {
        $pipeline->zadd('idx:' . $word, [$id => 0]);
    }

    $rvalue = to_ecpm(
        $type,
        [
            1000,
            isset($AVERAGE_PER_1K[$type]) ? $AVERAGE_PER_1K[$type] : 1,
            $value
        ]
    );
    $pipeline->hset('type:', $id, $type);
    $pipeline->zadd('idx:ad:value:', [$id => $rvalue]);
    $pipeline->zadd('ad:base_value:', [$id => $value]);
    $pipeline->sadd('terms:' . $id, array_values($words));
    $pipeline->execute();
}

function target_ad($conn, $locations, $content)
{
    $pipeline = $conn->pipeline(['atomic' => true]);

    list($matched_ads, $base_ecpm) = match_location($pipeline, $locations);
    list($words, $targeted_ads)    = finish_scoring(
        $pipeline, $matched_ads, $base_ecpm, $content
    );

    $pipeline->incr('ads:served:');
    $pipeline->zrevrange('idx:' . $targeted_ads, 0, 0);
    $results = $pipeline->execute();
    $targeted_ad = end($results);
    $target_id   = prev($results);

    if (!$targeted_ad) {
        return [null, null];
    }

    $ad_id = $targeted_ad[0];
    record_targeting_result($conn, $target_id, $ad_id, $words);

    return [$target_id, $ad_id];
}

function match_location($pipe, $locations)
{
    $required = array_map(
        function ($loc) { return 'req:' . $loc; },
        $locations
    );
    $matched_ads = union($pipe, $required, 300, false);

    return [
        $matched_ads,
        zintersect($pipe, [$matched_ads => 0, 'ad:value:' => 1], 30, ['_execute' => false])
    ];
}

function finish_scoring($pipe, $matched, $base, $content)
{
    $bonus_ecpm = [];
    $words = tokenize($content);
    foreach ($words as $word) {
        $word_bonus = zintersect(
            $pipe, [$matched => 0, $word => 1], 30, ['_execute' => false]
        );
        $bonus_ecpm[$word_bonus] = 1;
    }

    if ($bonus_ecpm) {
        $minimum = zunion(
            $pipe, $bonus_ecpm, 30, ['AGGREGATE' => 'MIN', '_execute' => false]
        );
        $maximum = zunion(
            $pipe, $bonus_ecpm, 30, ['AGGREGATE' => 'MAX', '_execute' => false]
        );

        return [
            $words,
            zunion(
                $pipe, [$base => 1, $minimum => .5, $maximum => .5], 30, ['_execute' => false]
            )
        ];
    }
}

function record_targeting_result($conn, $target_id, $ad_id, $words)
{
    $pipeline = $conn->pipeline(['atomic' => true]);
    
    $terms = $conn->smembers('terms:' . $ad_id);
    $matched = array_values(array_intersect($terms, $words));
    if ($matched) {
        $matched_key = sprintf('terms:matched:%s', $target_id);
        $pipeline->sadd($matched_key, $matched);
        $pipeline->expire($matched_key, 900);
    }

    $type = $conn->hget('type:', $ad_id);
    $pipeline->incr(sprintf('type:%s:views:', $type));
    foreach ($matched as $word) {
        $pipeline->zincrby(sprintf('views:%s', $ad_id), 1, $word);
    }
    $pipeline->zincrby(sprintf('views:%s', $ad_id), 1, '');

    $results = $pipeline->execute();
    if (!(end($results) % 100)) {
        update_cpms($conn, $ad_id);
    }
}

function record_click($conn, $target_id, $ad_id, $action = false)
{
    $pipeline = $conn->pipeline(['atomic' => true]);

    $click_key = sprintf('clicks:%s', $ad_id);
    $match_key = sprintf('terms:matched:%s', $target_id);

    $type = $conn->hget('type:', $ad_id);
    if ($type == 'cpa') {
        $pipeline->expire($match_key, 900);
        if ($action) {
            $click_key = sprintf('actions:%s', $ad_id);
        }
    }

    if ($action AND $type == 'cpa') {
        $pipeline->incr(sprintf('type:%s:actions:', $type));
    } else {
        $pipeline->incr(sprintf('type:%s:clicks:', $type));
    }

    $matched = $conn->smembers($match_key);
    array_push($matched, '');
    foreach ($matched as $word) {
        $pipeline->zincrby($click_key, 1, $word);
    }
    $pipeline->execute();

    update_cpms($conn, $ad_id);
}

function update_cpms($conn, $ad_id)
{
    global $AVERAGE_PER_1K;
    $pipeline = $conn->pipeline(['atomic' => true]);

    $pipeline->hget('type:', $ad_id);
    $pipeline->zscore('ad:base_value:', $ad_id);
    $pipeline->smembers('terms:' . $ad_id);
    list($type, $base_value, $words) = $pipeline->execute();

    $which = 'clicks';
    if ($type == 'cpa') {
        $which = 'actions';
    }

    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->get(sprintf('type:%s:views:', $type));
    $pipeline->get(sprintf('type:%s:%s', $type, $which));
    list($type_views, $type_clicks) = $pipeline->execute();
    $AVERAGE_PER_1K[$type] = (
        1000. * intval($type_clicks ?: '1') / intval($type_views ?: '1')
    );

    if ($type == 'cpm') {
        return;
    }

    $view_key  = sprintf('views:%s', $ad_id);
    $click_key = sprintf('%s:%s', $which, $ad_id);

    $pipeline->zscore($view_key, '');
    $pipeline->zscore($click_key, '');
    list($ad_views, $ad_clicks) = array_slice($pipeline->execute(), -2);
    if (($ad_clicks ?: 0) < 1) {
        $ad_ecpm = $conn->zscore('idx:ad:value:', $ad_id);
    } else {
        $ad_ecpm = to_ecpm($type, [$ad_views ?: 1, $ad_clicks ?: 0, $base_value]);
        $pipeline->zadd('idx:ad:value:', [$ad_id => $ad_ecpm]);
    }

    foreach ($words as $word) {
        $pipeline->zscore($view_key, $word);
        $pipeline->zscore($click_key, $word);
        list($views, $clicks) = array_slice($pipeline->execute(), -2);

        if (($clicks ?: 0) < 1) {
            continue;
        }

        $word_ecpm = to_ecpm($type, [$views ?: 1, $clicks ?: 0, $base_value]);
        $bonus = $word_ecpm - $ad_ecpm;
        $pipeline->zadd('idx:' . $word, [$ad_id => $bonus]);
    }
    $pipeline->execute();
}

function add_job($conn, $job_id, $required_skills)
{
    $conn->sadd('job:' . $job_id, $required_skills);
}

function is_qualified($conn, $job_id, $candidate_skills)
{
    $temp = Uuid::uuid4()->toString();
    $pipeline = $conn->pipeline(['atomic' => true]);
    $pipeline->sadd($temp, $candidate_skills);
    $pipeline->expire($temp, 5);
    $pipeline->sdiff('job:' . $job_id, $temp);
    $results = $pipeline->execute();

    return !end($results);
}

function index_job($conn, $job_id, $skills)
{
    $pipeline = $conn->pipeline(['atomic' => true]);
    foreach ($skills as $skill) {
        $pipeline->sadd('idx:skill:' . $skill, $job_id);
    }
    $pipeline->zadd('idx:jobs:req', [$job_id => count($skills)]);
    $pipeline->execute();
}

function find_jobs($conn, $candidate_skills)
{
    $skills = [];
    foreach ($candidate_skills as $skill) {
        $skills['skill:' . $skill] = 1;
    }
    $job_scores   = zunion($conn, $skills);
    $final_result = zintersect($conn, [$job_scores => -1, 'jobs:req' => 1]);

    return $conn->zrangebyscore('idx:' . $final_result, 0, 0);
}

const SKILL_LEVEL_LIMIT = 2;

function index_job_levels($conn, $job_id, $skill_levels)
{
    $total_skills = count(array_keys($skill_levels));
    $pipeline = $conn->pipeline(['atomic' => true]);
    foreach ($skill_levels as $skill => $level) {
        $level = min($level, SKILL_LEVEL_LIMIT);
        foreach (range($level, SKILL_LEVEL_LIMIT) as $wlevel) {
            $pipeline->sadd(sprintf('idx:skill:%s:%s', $skill, $wlevel), $job_id);
        }
    }
    $pipeline->zadd('idx:jobs:req', [$job_id => $total_skills]);
    $pipeline->execute();
}

function search_job_levels($conn, $skill_levels)
{
    $skills = [];
    foreach ($skill_levels as $skill => $level) {
        $level = min($level, SKILL_LEVEL_LIMIT);
        $skills[sprintf('skill:%s:%s', $skill, $level)] = 1;
    }

    $job_scores    = zunion($conn, $skills);
    $final_results = zintersect($conn, [$job_scores => -1, 'jobs:req' => 1]);

    return $conn->zrangebyscore('idx:' . $final_results, '-inf', 0);
}

function index_job_years($conn, $job_id, $skill_years)
{
    $total_skills = count(array_keys($skill_years));
    $pipeline = $conn->pipeline(['atomic' => true]);

    foreach ($skill_years as $skill => $years) {
        $pipeline->zadd(
            sprintf('idx:skill:%s:years', $skill), [$job_id => max($years, 0)]
        );
    }
    $pipeline->sadd('idx:jobs:all', $job_id);
    $pipeline->zadd('idx:jobs:req', [$job_id => $total_skills]);
    $pipeline->execute();
}

function search_job_years($conn, $skill_years)
{
    $pipeline = $conn->pipeline(['atomic' => true]);

    $union = [];
    foreach ($skill_years as $skill => $years) {
        $sub_result = zintersect(
            $pipeline,
            ['jobs:all' => -$years, sprintf('skill:%s:years', $skill) => 1],
            30,
            ['_execute' => false]
        );
        $pipeline->zremrangebyscore('idx:' . $sub_result, '(0', 'inf');
        $union[] = zintersect(
            $pipeline,
            ['jobs:all' => 1, $sub_result => 0],
            30,
            ['_execute' => false]
        );
    }

    $job_scores = zunion(
        $pipeline,
        array_fill_keys(array_values($union), 1),
        30,
        ['_execute' => false]
    );
    $final_result = zintersect(
        $pipeline,
        [$job_scores => -1, 'jobs:req' => 1],
        30,
        ['_execute' => false]
    );

    $pipeline->zrangebyscore('idx:' . $final_result, '-inf', 0);
    $results = $pipeline->execute();

    return end($results);
}