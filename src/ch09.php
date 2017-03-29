<?php

namespace RedisInAction\Ch09;

use DateTime;
use Ramsey\Uuid\Uuid;

use function RedisInAction\Helper\bisect_left;
use function RedisInAction\Ch06\readblocks;

function long_ziplist_performance($conn, $key, $length, $passes, $psize)
{
    $conn->del($key);
    $conn->rpush($key, range(0, $length - 1));
    $pipeline = $conn->pipeline(['atomic' => false]);

    $t = microtime(true);
    for ($p = 0; $p < $passes; $p++) {
        for ($pi = 0; $pi < $psize; $pi++) {
            $pipeline->rpoplpush($key, $key);
        }
        $pipeline->execute();
    }

    return ($passes * $psize) / ((microtime(true) - $t) ?: .001);
}

function long_ziplist_index($conn, $key, $length, $passes, $psize)
{
    $conn->del($key);
    $conn->rpush($key, range(0, $length -1));
    $length >>= 1;
    $pipeline = $conn->pipeline(['atomic' => false]);
    $t = microtime(true);
    for ($p = 0; $p < $passes; $p++) {
        for ($pi = 0; $pi < $psize; $pi++) {
            $pipeline->lindex($key, $length);
        }
        $pipeline->execute();
    }

    return ($passes * $psize) / ((microtime(true) - $t) ?: .001);
}

function long_intset_performance($conn, $key, $length, $passes, $psize)
{
    $conn->del($key);
    $conn->rpush($key, range(1000000, 1000000 + $length -1));
    $cur = 1000000 - 1;
    $pipeline = $conn->pipeline(['atomic' => false]);
    $t = microtime(true);
    for ($p = 0; $p < $passes; $p++) {
        for ($pi = 0; $pi < $psize; $pi++) {
            $pipeline->spop($key);
            $pipeline->sadd($key, $cur);
        }
        $pipeline->execute();
    }

    return ($passes * $psize) / ((microtime(true) - $t) ?: .001);
}

function shard_key($base, $key, $total_elements, $shard_size)
{
    if (is_int($key) OR is_float($key) OR ctype_digit($key)) {
        $shard_id = floor(intval(strval($key), 10) / $shard_size);
    } else {
        $shards   = floor(2 * $total_elements / $shard_size);
        $shard_id = crc32($key) % $shards;
    }

    return sprintf('%s:%s', $base, $shard_id);
}

function shard_hset($conn, $base, $key, $value, $total_elements, $shard_size)
{
    $shard = shard_key($base, $key, $total_elements, $shard_size);

    return $conn->hset($shard, $key, $value);
}

function shard_hget($conn, $base, $key, $total_elements, $shard_size)
{
    $shard = shard_key($base, $key, $total_elements, $shard_size);

    return $conn->hget($shard, $key);
}

function shard_sadd($conn, $base, $member, $total_elements, $shard_size)
{
    $shard = shard_key($base, 'x' . $member, $total_elements, $shard_size);

    return $conn->sadd($shard, $member);
}

const SHARD_SIZE = 512;

function count_visit($conn, $session_id)
{
    $today    = new DateTime();
    $key      = sprintf('unique:%s', $today->format('Y-m-d'));
    $expected = get_expected($conn, $key, $today);

    $id = intval(substr(str_replace('-', '', $session_id), 0, 15), 16);
    if (shard_sadd($conn, $key, $id, $expected, SHARD_SIZE)) {
        $conn->incr($key);
    }
}

global $DAILY_EXPECTED, $EXPECTED;

$DAILY_EXPECTED = 1000000;
$EXPECTED = [];

function get_expected($conn, $key, $today)
{
    global $DAILY_EXPECTED, $EXPECTED;

    if (array_key_exists($key, $EXPECTED)) {
        return $EXPECTED[$key];
    }

    $exkey    = $key . ':expected';
    $expected = $conn->get($exkey);

    if (!$expected) {
        $yesterday = $today->modify('-1 day');
        $expected  = $conn->get(
            sprintf('unique:%s', $yesterday->format('Y-m-d'))
        );
        $expected = intval($expected ?: $DAILY_EXPECTED);
        $expected = 2 ** intval(ceil(log($expected * 1.5, 2)));
        if (!$conn->setnx($exkey, $expected)) {
            $expected = $conn->get($exkey);
        }
    }

    $EXPECTED[$key] = intval($expected);

    return $EXPECTED[$key];
}

global $COUNTRIES, $STATES;

$COUNTRIES = [
    'ABW', 'AFG', 'AGO', 'AIA', 'ALA', 'ALB', 'AND', 'ARE', 'ARG', 'ARM', 'ASM',
    'ATA', 'ATF', 'ATG', 'AUS', 'AUT', 'AZE', 'BDI', 'BEL', 'BEN', 'BES', 'BFA',
    'BGD', 'BGR', 'BHR', 'BHS', 'BIH', 'BLM', 'BLR', 'BLZ', 'BMU', 'BOL', 'BRA',
    'BRB', 'BRN', 'BTN', 'BVT', 'BWA', 'CAF', 'CAN', 'CCK', 'CHE', 'CHL', 'CHN',
    'CIV', 'CMR', 'COD', 'COG', 'COK', 'COL', 'COM', 'CPV', 'CRI', 'CUB', 'CUW',
    'CXR', 'CYM', 'CYP', 'CZE', 'DEU', 'DJI', 'DMA', 'DNK', 'DOM', 'DZA', 'ECU',
    'EGY', 'ERI', 'ESH', 'ESP', 'EST', 'ETH', 'FIN', 'FJI', 'FLK', 'FRA', 'FRO',
    'FSM', 'GAB', 'GBR', 'GEO', 'GGY', 'GHA', 'GIB', 'GIN', 'GLP', 'GMB', 'GNB',
    'GNQ', 'GRC', 'GRD', 'GRL', 'GTM', 'GUF', 'GUM', 'GUY', 'HKG', 'HMD', 'HND',
    'HRV', 'HTI', 'HUN', 'IDN', 'IMN', 'IND', 'IOT', 'IRL', 'IRN', 'IRQ', 'ISL',
    'ISR', 'ITA', 'JAM', 'JEY', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ', 'KHM', 'KIR',
    'KNA', 'KOR', 'KWT', 'LAO', 'LBN', 'LBR', 'LBY', 'LCA', 'LIE', 'LKA', 'LSO',
    'LTU', 'LUX', 'LVA', 'MAC', 'MAF', 'MAR', 'MCO', 'MDA', 'MDG', 'MDV', 'MEX',
    'MHL', 'MKD', 'MLI', 'MLT', 'MMR', 'MNE', 'MNG', 'MNP', 'MOZ', 'MRT', 'MSR',
    'MTQ', 'MUS', 'MWI', 'MYS', 'MYT', 'NAM', 'NCL', 'NER', 'NFK', 'NGA', 'NIC',
    'NIU', 'NLD', 'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAK', 'PAN', 'PCN', 'PER',
    'PHL', 'PLW', 'PNG', 'POL', 'PRI', 'PRK', 'PRT', 'PRY', 'PSE', 'PYF', 'QAT',
    'REU', 'ROU', 'RUS', 'RWA', 'SAU', 'SDN', 'SEN', 'SGP', 'SGS', 'SHN', 'SJM',
    'SLB', 'SLE', 'SLV', 'SMR', 'SOM', 'SPM', 'SRB', 'SSD', 'STP', 'SUR', 'SVK',
    'SVN', 'SWE', 'SWZ', 'SXM', 'SYC', 'SYR', 'TCA', 'TCD', 'TGO', 'THA', 'TJK',
    'TKL', 'TKM', 'TLS', 'TON', 'TTO', 'TUN', 'TUR', 'TUV', 'TWN', 'TZA', 'UGA',
    'UKR', 'UMI', 'URY', 'USA', 'UZB', 'VAT', 'VCT', 'VEN', 'VGB', 'VIR', 'VNM',
    'VUT', 'WLF', 'WSM', 'YEM', 'ZAF', 'ZMB', 'ZWE'
];

$STATES = [
    'CAN' => [
        'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 'NU', 'ON', 'PE', 'QC', 'SK',
        'YT',
    ],
    'USA' => [
        'AA', 'AE', 'AK', 'AL', 'AP', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC',
        'DE', 'FL', 'FM', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY',
        'LA', 'MA', 'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC',
        'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR',
        'PW', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI',
        'WV', 'WY',
    ],
];

function get_code($country, $state)
{
    global $COUNTRIES, $STATES;

    $cindex = bisect_left($COUNTRIES, $country);
    if ($cindex > count($COUNTRIES) OR $COUNTRIES[$cindex] != $country) {
        $cindex = -1;
    }
    $cindex += 1;

    $sindex = -1;
    if ($state AND array_key_exists($country, $STATES)) {
        $states = $STATES[$country];
        $sindex = bisect_left($states, $state);
        if ($sindex > count($states) OR $states[$sindex] != $state) {
            $sindex = -1;
        }
    }
    $sindex += 1;

    return chr($cindex) . chr($sindex);
}

const USERS_PER_SHARD = 2 ** 20;

function set_location($conn, $user_id, $country, $state)
{
    $code = get_code($country, $state);

    $shard_id = intval($user_id / USERS_PER_SHARD);
    $position = $user_id % USERS_PER_SHARD;
    $offset   = $position * 2;

    $pipe = $conn->pipeline(['atomic' => false]);
    $pipe->setrange(sprintf('location:%s', $shard_id), $offset, $code);

    $tkey = Uuid::uuid4()->toString();

    $pipe->zadd($tkey, ['max' => $user_id]);
    $pipe->zunionstore(
        'location:max', [$tkey, 'location:max'], ['AGGREGATE' => 'MAX']
    );
    $pipe->del($tkey);

    $pipe->execute();
}

function aggregate_location($conn)
{
    $countries = [];
    $states    = [];

    $max_id    = intval($conn->zscore('location:max', 'max'));
    $max_block = intval($max_id / USERS_PER_SHARD);

    foreach (range(0, $max_block) as $shard_id) {
        foreach (
            readblocks($conn, sprintf('location:%s', $shard_id)) as $block
        ) {
            foreach (range(0, strlen($block) - 2, 2) as $offset) {
                $code = substr($block, $offset, 2);
                update_aggregates($countries, $states, [$code]);
            }
        }
    }

    return [$countries, $states];
}

function update_aggregates(&$countries, &$states, $codes)
{
    global $COUNTRIES, $STATES;

    foreach ($codes as $code) {
        if (strlen($code) != 2) {
            continue;
        }

        $country = ord($code[0]) - 1;
        $state   = ord($code[1]) - 1;

        if ($country < 0 OR $country >= count($COUNTRIES)) {
            continue;
        }

        $country = $COUNTRIES[$country];
        if (!isset($countries[$country])) {
            $countries[$country] = 0;
        }
        $countries[$country] += 1;

        if (!array_key_exists($country, $STATES)) {
            continue;
        }

        if ($state < 0 OR $state >= count($STATES[$country])) {
            continue;
        }

        $state = $STATES[$country][$state];
        if (!isset($states[$country][$state])) {
            $states[$country][$state] = 0;
        }
        $states[$country][$state] += 1;
    }
}

function aggregate_location_list($conn, $user_ids)
{
    $pipe = $conn->pipeline(['atomic' => false]);
    $countries = [];
    $states    = [];

    foreach ($user_ids as $i => $user_id) {
        $shard_id = intval($user_id / USERS_PER_SHARD);
        $position = $user_id % USERS_PER_SHARD;
        $offset   = $position * 2;

        $pipe->substr(sprintf('location:%s', $shard_id), $offset, $offset + 1);

        if (($i + 1) % 1000 == 0) {
            update_aggregates($countries, $states, $pipe->execute());
            // re-enter pipeline to discard previous responses
            $pipe = $conn->pipeline(['atomic' => false]);
        }
    }
    update_aggregates($countries, $states, $pipe->execute());

    return [$countries, $states];
}