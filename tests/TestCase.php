<?php

namespace RedisInAction\Test;

use PHPUnit\Framework\TestCase as PHPUnitTestCase;
use Predis\Client as RedisClient;

class TestCase extends PHPUnitTestCase
{
    protected $conn;

    protected function setUp()
    {
        $this->conn = new RedisClient([
            'host' => $this->getRedisServerHost(),
            'port' => $this->getRedisServerPort(),
        ]);
    }

    protected function tearDown()
    {
        unset($this->conn);
    }

    protected function getRedisServerHost()
    {
        return isset($GLOBALS['REDIS_HOST']) ? $GLOBALS['REDIS_HOST'] : '127.0.0.1';
    }

    protected function getRedisServerPort()
    {
        return isset($GLOBALS['REDIS_PORT']) ? $GLOBALS['REDIS_PORT'] : '6379';
    }

    /**
     * Helper method to print message or data
     *
     * @see http://stackoverflow.com/a/12606210/1519894
     *
     * @author Sam Lu
     *
     * @param mixed $message
     */
    protected static function pprint($message = PHP_EOL)
    {
        fwrite(STDERR, trim(print_r($message, true), PHP_EOL) . PHP_EOL);
    }
}