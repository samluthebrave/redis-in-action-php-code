<?php

namespace RedisInAction\Ch09;

use RedisInAction\Test\TestCase;
use Ramsey\Uuid\Uuid;

class Ch09Test extends TestCase
{
    protected function setUp()
    {
        parent::setUp();

        $this->conn->flushDb();
    }

    protected function tearDown()
    {
        $this->conn->flushDb();

        parent::tearDown();
    }

    public function test_long_ziplist_performance()
    {
        long_ziplist_performance($this->conn, 'test', 5, 10, 10);
        $this->assertEquals(5, $this->conn->llen('test'));
    }

    public function test_shard_key()
    {
        $base = 'test';
        $this->assertEquals('test:0', shard_key($base, 1, 2, 2));
        $this->assertEquals('test:0', shard_key($base, '1', 2, 2));
        $this->assertEquals('test:1', shard_key($base, 125, 1000, 100));
        $this->assertEquals('test:1', shard_key($base, '125', 1000, 100));

        for ($i = 0; $i < 50; $i++) {
            $parts  = explode(
                ':', shard_key($base, sprintf('hello:%s', $i), 1000, 100)
            );
            $actual = intval(end($parts));
            $this->assertTrue(0 <= $actual && $actual < 20);

            $parts  = explode(':', shard_key($base, $i, 1000, 100));
            $actual = intval(end($parts));
            $this->assertTrue(0 <= $actual && $actual < 10);
        }
    }

    public function test_sharded_hash()
    {
        for ($i = 0; $i < 50; $i++) {
            shard_hset($this->conn, 'test', sprintf('keyname:%s', $i), $i, 1000, 100);
            $this->assertEquals(
                strval($i),
                shard_hget($this->conn, 'test', sprintf('keyname:%s', $i), 1000, 100)
            );
            shard_hset($this->conn, 'test2', $i, $i, 1000, 100);
            $this->assertEquals(
                strval($i),
                shard_hget($this->conn, 'test2', $i, 1000, 100)
            );
        }
    }

    public function test_sharded_sadd()
    {
        for ($i = 0; $i < 50; $i++) {
            shard_sadd($this->conn, 'testx', $i, 50, 50);
        }
        $this->assertEquals(
            50,
            $this->conn->scard('testx:0') + $this->conn->scard('testx:1')
        );
    }

    public function test_unique_visitors()
    {
        global $DAILY_EXPECTED;
        $DAILY_EXPECTED = 10000;

        for ($i = 0; $i < 179; $i++) {
            count_visit($this->conn, Uuid::uuid4()->toString());
        }
        $this->assertEquals(
            '179',
            $this->conn->get(sprintf('unique:%s', date('Y-m-d')))
        );

        $this->conn->flushdb();

        $this->conn->set(
            sprintf('unique:%s', date('Y-m-d', strtotime('-1 day'))), 1000
        );

        for ($i = 0; $i < 183; $i++) {
            count_visit($this->conn, Uuid::uuid4()->toString());
        }
        $this->assertEquals(
            '183',
            $this->conn->get(sprintf('unique:%s', date('Y-m-d')))
        );
    }

    public function test_user_location()
    {
        $i = 0;
        foreach (COUNTRIES as $country) {
            if (array_key_exists($country, STATES)) {
                foreach (STATES[$country] as $state) {
                    set_location($this->conn, $i, $country, $state);
                    $i += 1;
                }
            } else {
                set_location($this->conn, $i, $country, '');
                $i += 1;
            }
        }

        list($_countries, $_states) = aggregate_location($this->conn);
        list($countries, $states) = aggregate_location_list(
            $this->conn, range(0, $i)
        );

        $this->assertEquals($_countries, $countries);
        $this->assertEquals($_states, $states);

        foreach (array_keys($countries) as $c) {
            if (array_key_exists($c, STATES)) {
                $this->assertEquals(count(STATES[$c]), $countries[$c]);
                foreach (STATES[$c] as $s) {
                    $this->assertEquals(1, $states[$c][$s]);
                }
            } else {
                $this->assertEquals(1, $countries[$c]);
            }
        }
    }
}