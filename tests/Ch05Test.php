<?php

namespace RedisInAction\Ch05;

use RedisInAction\Helper\Threading;
use RedisInAction\Test\TestCase;

class Ch05Test extends TestCase
{
    protected function setUp()
    {
        parent::setUp();

        global $CONFIG_CONNECTION;

        $CONFIG_CONNECTION = $this->conn;

        $this->conn->flushDb();
    }

    protected function tearDown()
    {
//        $this->conn->flushDb();
        unset($this->conn);
        global $CONFIG_CONNECTION, $QUIT, $SAMPLE_COUNT;
        $CONFIG_CONNECTION = null;
        $QUIT = false;
        $SAMPLE_COUNT = 100;
        self::pprint();
        self::pprint();
    }

    public function test_log_recent()
    {
        $conn = $this->conn;

        self::pprint("Let's write a few logs to the recent log");
        for ($msg = 0; $msg < 5; $msg++) {
            log_recent($conn, 'test', sprintf('this is message %s', $msg));
        }
        $recent = $conn->lrange('recent:test:info', 0, -1);
        self::pprint("The current recent message log has this many messages: " . count($recent));
        self::pprint("Those messages include:");
        self::pprint(array_slice($recent, 0, 10));
        $this->assertTrue(count($recent) >= 5);
    }

    public function test_log_common()
    {
        $conn = $this->conn;

        self::pprint("Let's write some items to the common log");
        for ($count = 1; $count < 6; $count++) {
            for ($i = 0; $i < $count; $i++) {
                log_common($conn, 'test', sprintf("msssage-%s", $count));
            }
        }
        $common = $conn->zrevrange('common:test:info', 0, -1, ['WITHSCORES' => true]);
        self::pprint("The current number of common messages is: " . count($common));
        self::pprint("Those common messages are:");
        self::pprint($common);
        $this->assertTrue(count($common) >= 5);
    }

    public function test_counters()
    {
        global $QUIT, $SAMPLE_COUNT;
        $conn = $this->conn;

        self::pprint("Let's update some counters for now and a little in the future");
        $now = microtime(true);
        for ($delta = 0; $delta < 10; $delta++) {
            update_counter($conn, 'test', mt_rand(1, 5), $now + $delta);
        }
        $counter = get_counter($conn, 'test', 1);
        self::pprint("We have some per-second counters: " . count($counter));
        $this->assertTrue(count($counter) >= 10);
        $counter = get_counter($conn, 'test', 5);
        self::pprint("We have some per-5-second counters: " . count($counter));
        self::pprint("These counters include:");
        self::pprint(array_slice($counter, 0, 10));
        $this->assertTrue(count($counter) >= 2);
        self::pprint();

        self::pprint("Let's clean out some counters by setting our sample count to 0");
        $SAMPLE_COUNT = 0;
        $t = new Threading(
            __NAMESPACE__ . '\clean_counters',
            [$conn, 2 * 86400],
            ['QUIT' => $QUIT, 'SAMPLE_COUNT' => $SAMPLE_COUNT]
        );
        $t->start();
        sleep(1);
        $t->setGlobal('QUIT', $QUIT = true);

        $t->join();

        $counter = get_counter($conn, 'test', 86400);
        self::pprint("Did we clean out all of the counters? " . json_encode(!$counter));
        $this->assertEmpty($counter);
    }

    public function test_ip_lookup()
    {
        $conn = $this->conn;

        try {
            fopen('GeoLiteCity-Blocks.csv', 'rb');
            fopen('GeoLiteCity-Location.csv', 'rb');
        } catch (\Exception $e) {
            self::pprint("********");
            self::pprint("You do not have the GeoLiteCity database available, aborting test");
            self::pprint("Please have the following two files in the current path:");
            self::pprint("GeoLiteCity-Blocks.csv");
            self::pprint("GeoLiteCity-Location.csv");
            self::pprint("********");
            return;
        }

        self::pprint("Importing IP addresses to Redis... (this may take a while)");
        import_ips_to_redis($conn, 'GeoLiteCity-Blocks.csv');
        $ranges = $conn->zcard('ip2cityid:');
        self::pprint("Loaded ranges into Redis: " . $ranges);
        $this->assertTrue($ranges > 1000);
        self::pprint();

        self::pprint("Importing Location lookups to Redis... (this may take a while)");
        import_cities_to_redis($conn, 'GeoLiteCity-Location.csv');
        $cities = $conn->hlen('cityid2city:');
        self::pprint("Loaded city lookups into Redis: " . $cities);
        $this->assertTrue($cities > 1000);
        self::pprint();

        self::pprint("Let's lookup some locations!");
        for ($i = 0; $i < 5; $i++) {
            $city = find_city_by_ip(
                $conn ,
                sprintf('%s.%s.%s.%s', mt_rand(1, 255), mt_rand(0, 255), mt_rand(0, 255), mt_rand(0, 255))
            );

            self::pprint($city);
        }
    }

    public function test_is_under_maintenance()
    {
        self::pprint(
            "Are we under maintenance (we shouldn't be)? "
            . json_encode(is_under_maintenance($this->conn))
        );
        $this->conn->set('is-under-maintenance', 'yes');
        self::pprint(
            "We cached this, so it should be the same: "
            . json_encode(is_under_maintenance($this->conn))
        );
        sleep(1);
        self::pprint(
            "But after a sleep, it should change: "
            . json_encode(is_under_maintenance($this->conn))
        );
        self::pprint("Cleaning up...");
        $this->conn->del('is-under-maintenance');
        sleep(1);
        self::pprint(
            "Should be False again:"
            . json_encode(is_under_maintenance($this->conn))
        );
    }

    public function test_config()
    {
        self::pprint("Let's set a config and then get a connection from that config...");

        set_config(
            $this->conn,
            'redis',
            'test',
            [
                'host'     => $this->getRedisServerHost(),
                'port'     => $this->getRedisServerPort(),
                'database' => 15
            ]
        );
        $test = function ($conn2) {
            return boolval($conn2->info());
        };
        $conn2 = redis_connection('test', $wait = 1, $test);
        self::pprint(
            "We can run commands from the configured connection: "
            . json_encode($test($conn2))
        );
    }
}