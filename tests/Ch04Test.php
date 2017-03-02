<?php

namespace RedisInAction\Ch04;

use RedisInAction\Test\TestCase;

class Ch04Test extends TestCase
{
    public function test_list_item()
    {
        $conn = $this->conn;

        self::pprint("We need to set up just enough state so that a user can list an item");
        $seller = 'userX';
        $item = 'itemX';
        $conn->sadd('inventory:' . $seller, $item);
        $i = $conn->smembers('inventory:' . $seller);
        self::pprint("The user's inventory has: ");
        self::pprint($i);
        $this->assertNotEmpty($i);
        self::pprint();

        self::pprint("Listing the item...");
        $l = list_item($conn, $item, $seller, 10);
        self::pprint("Listing the item succeeded?");
        self::pprint($l);
        $this->assertTrue($l);
        $r = $conn->zrange('market:', 0, -1, ['WITHSCORES' => true]);
        self::pprint("The market contains:");
        self::pprint($r);
        $this->assertNotEmpty($r);
        $this->assertTrue(array_key_exists('itemX.userX', $r));
    }

    public function test_purchase_item()
    {
        $this->test_list_item();
        $conn = $this->conn;

        self::pprint("We need to set up just enough state so a user can buy an item");
        $buyer = 'userY';
        $conn->hset('users:userY', 'funds', 125);
        $r = $conn->hgetall('users:userY');
        self::pprint("The user has some money:");
        self::pprint($r);
        $this->assertNotEmpty($r);
        $this->assertNotEmpty($r['funds']);
        self::pprint();

        self::pprint("Let's purchase an item");
        $p = purchase_item($conn, 'userY', 'itemX', 'userX', 10);
        self::pprint("Purchasing an item succeeded?");
        self::pprint($p);
        $this->assertTrue($p);
        $r = $conn->hgetall('users:userY');
        self::pprint("Their money is now:");
        self::pprint($r);
        $this->assertNotEmpty($r);
        $i = $conn->smembers('inventory:' . $buyer);
        self::pprint("Their inventory is now:");
        self::pprint($i);
        $this->assertNotEmpty($i);
        $this->assertTrue(in_array('itemX', $i));
        // or you can use `assertNull()`
        $this->assertEquals($conn->zscore('market:', 'itemX.userX'), null);
    }

    public function test_benchmark_update_token()
    {
        benchmark_update_token($this->conn, 5);
    }
}