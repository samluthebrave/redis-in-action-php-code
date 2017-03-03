<?php

namespace RedisInAction\Ch05;

use Predis\Client as RedisClient;
use Predis\Transaction\AbortedMultiExecException;
use Psr\Log\LogLevel as Severity;
use Mannion007\PhpBinarySearch\ArraySearch;
use RedisInAction\Helper\Threading;

global $QUIT, $SAMPLE_COUNT;
$QUIT         = false;
$SAMPLE_COUNT = 100;

function log_recent($conn, $name, $message, $severity = Severity::INFO, $pipe = null)
{
    $severity    = strtolower($severity);
    $destination = sprintf('recent:%s:%s', $name, $severity);
    $message     = date('D M d H:i:s Y') . ' ' . $message;

    $pipe = $pipe ?: $conn->pipeline();
    $pipe->lpush($destination, $message);
    $pipe->ltrim($destination, 0, 99);
    $pipe->execute();
}

function log_common($conn, $name, $message, $severity = Severity::INFO, $timeout = 5)
{
    $severity    = strtolower($severity);
    $destination = sprintf('common:%s:%s', $name, $severity);
    $start_key   = $destination . ':start';
    $trans       = $conn->transaction(['cas' => true]);
    $end         = microtime(true) + $timeout;
    while (microtime(true) < $end) {
        try {
            $trans->watch($start_key);
            $hour_start = gmdate('Y-m-d\TH:i:00'); // example: 2017-01-21T06:00:00

            $existing = $trans->get($start_key);
            $trans->multi();
            if ($existing AND $existing < $hour_start) {
                $trans->rename($destination, $destination . ':last');
                $trans->rename($start_key, $destination . ':pstart');
                $trans->set($start_key, $hour_start);
            } elseif (!$existing) {
                $trans->set($start_key, $hour_start);
            }

            $trans->zincrby($destination, 1, $message);
            log_recent($trans, $name, $message, $severity, $trans);

            return;
        } catch (AbortedMultiExecException $e) {
            continue;
        }
    }
}

// array constants require PHP 5.6
const PRECISION = [1, 5, 60, 300, 3600, 18000, 86400];

function update_counter($conn, $name, $count = 1, $now = null)
{
    $now  = $now ?: microtime(true);
    $pipe = $conn->pipeline(['atomic' => true]);
    foreach (PRECISION as $prec) {
        $pnow = intval($now / $prec) * $prec;
        $hash = sprintf('%s:%s', $prec, $name);
        $pipe->zadd('known:', [$hash => 0]);
        $pipe->hincrby('count:' . $hash, $pnow, $count);
    }
    $pipe->execute();
}

function get_counter($conn, $name, $precision)
{
    $hash      = sprintf('%s:%s', $precision, $name);
    $data      = $conn->hgetall('count:' . $hash);
    $to_return = [];
    foreach ($data as $key => $value) {
        $to_return[] = [intval($key), intval($value)];
    }
    sort($to_return);

    return $to_return;
}

function clean_counters($conn, Threading $thread)
{
    $trans  = $conn->transaction(['cas' => true]);
    $passes = 0;

    while (!$thread->getGlobal('QUIT')) {
        $start = microtime(true);
        $index = 0;
        while ($index < $conn->zcard('known:')) {
            $hash = $conn->zrange('known:', $index, $index);
            $index += 1;
            if (!$hash) {
                break;
            }

            $hash  = $hash[0];
            $prec  = intval(explode(':', $hash)[0]);
            $bprec = intval(floor($prec / 60)) ?: 1;
            if ($passes % $bprec) {
                continue;
            }

            $hkey    = 'count:' . $hash;
            $cutoff  = microtime(true) + $thread->getGlobal('SAMPLE_COUNT') * $prec;
            $samples = array_map('intval', $conn->hkeys($hkey));
            sort($samples);
            $cmp = function ($a, $b)
            {
                if ($a == $b) {
                    return 0;
                }
                return ($a < $b) ? -1 : 1;
            };
            $remove = ArraySearch::binarySearch($cutoff, $samples, $cmp, count($samples) - 1);
            if ($remove) {
                $conn->hdel($hkey, array_slice($samples, 0, $remove));
                if ($remove == count($samples)) {
                    try {
                        $trans->watch($hkey);
                        if (!$trans->hlen($hkey)) {
                            $trans->multi();
                            $trans->zrem('known:', $hash);
                            $trans->execute();
                            $index -= 1;
                        } else {
                            $trans->unwatch();
                        }
                    } catch (AbortedMultiExecException $e) {
                        // pass
                    }
                }
            }
        }
        $passes += 1;
        $duration = min(intval(microtime(true) - $start) + 1, 60);
        sleep(max((60 - $duration), 1));
    }
}

function ip_to_score($ip_address)
{
    $score = 0;
    foreach (explode('.', $ip_address) as $v) {
        $score = $score * 256 + intval($v, 10);
    }

    return $score;
}

function import_ips_to_redis($conn, $filename)
{
    $handle = fopen($filename, 'rb');
    $count  = 0;
    while (($row = fgetcsv($handle)) !== false) {
        $start_ip = $row ? $row[0] : '';
        if (strpos(strtolower($start_ip), 'i') !== false) {
            continue;
        }
        if (strpos($start_ip, '.') !== false) {
            $start_ip = ip_to_score($start_ip);

        // though `is_numeric()` behaves differently than Python's `str.isdigit()`
        // in this context they can be seen as same
        } elseif (is_numeric($start_ip)) {
            $start_ip = intval($start_ip, 10);
        } else {
            continue;
        }

        $city_id = $row[2] . '_' . strval($count);
        $conn->zadd('ip2cityid:', [$city_id => $start_ip]);

        $count++;
    }

    fclose($handle);
}

function import_cities_to_redis($conn, $filename)
{
    $handle = fopen($filename, 'rb');
    while (($row = fgetcsv($handle)) !== false) {
        if (count($row) < 4 OR !is_numeric($row[0])) {
            continue;
        }
        $row = array_map(
            function ($v) {
                return iconv('ISO-8859-1', 'UTF-8', $v); // or `utf8_encode()`
            },
            $row
        );
        $city_id = $row[0];
        $country = $row[1];
        $region  = $row[2];
        $city    = $row[3];
        $conn->hset('cityid2city:', $city_id, json_encode([$city, $region, $country]));
    }
}

function find_city_by_ip($conn, $ip_address)
{
    if (is_string($ip_address)) {
        $ip_address = ip_to_score($ip_address);
    }

    $city_id = $conn->zrevrangebyscore(
        'ip2cityid:', $ip_address, 0, ['LIMIT' => [0, 1]]
    );

    if (!$city_id) {
        return null;
    }

    $city_id = explode('_', $city_id[0])[0];

    return json_decode($conn->hget('cityid2city:', $city_id), true);
}

global $LAST_CHECKED, $IS_UNDER_MAINTENANCE;

$LAST_CHECKED         = null;
$IS_UNDER_MAINTENANCE = false;

function is_under_maintenance($conn)
{
    global $LAST_CHECKED, $IS_UNDER_MAINTENANCE;

    if ($LAST_CHECKED < microtime(true) - 1) {
        $LAST_CHECKED         = microtime(true);
        $IS_UNDER_MAINTENANCE = boolval($conn->get('is-under-maintenance'));
    }

    return $IS_UNDER_MAINTENANCE;
}

function set_config($conn, $type, $component, $config)
{
    $conn->set(sprintf('config:%s:%s', $type, $component), json_encode($config));
}

global $CONFIGS, $CHECKED;

$CONFIGS = [];
$CHECKED = [];

function get_config($conn, $type, $component, $wait = 1)
{
    $key = sprintf('config:%s:%s', $type, $component);

    global $CONFIGS, $CHECKED;
    if (!isset($CHECKED[$key]) OR $CHECKED[$key] < (microtime(true) - $wait)) {
        $CHECKED[$key] = microtime(true);

        $config = json_decode(($conn->get($key) ?: '{}'), true);
        array_walk(
            $config,
            function (&$value, &$key) { strval($key); }
        );
        $old_config = isset($CONFIGS[$key]) ? $CONFIGS[$key] : [];

        if ($config != $old_config) {
            $CONFIGS[$key] = $config;
        }
    }

    return $CONFIGS[$key];
}

global $CONFIG_CONNECTION, $REDIS_CONNECTIONS;

$CONFIG_CONNECTION = null;
$REDIS_CONNECTIONS = [];

/**
 * It's hardly possible to mimic Python's decorator,
 * so this function simply return the connection.
 *
 * @author Sam Lu
 *
 * @param string $component
 * @param int    $wait
 *
 * @return \Predis\Client $connection
 */
function redis_connection($component, $wait = 1)
{
    $key = sprintf('config:redis:' . $component);

    global $CONFIGS, $CONFIG_CONNECTION, $REDIS_CONNECTIONS;

    $old_config = isset($CONFIGS[$key]) ? $CONFIGS[$key] : [];
    $_config    = get_config($CONFIG_CONNECTION, 'redis', $component, $wait);
    $config     = [];
    foreach ($_config as $k => $v) {
        $config[utf8_encode($k)] = $v;
    }

    if ($config != $old_config) {
        $REDIS_CONNECTIONS[$key] = new RedisClient($config);
    }

    return $REDIS_CONNECTIONS[$key];
}



