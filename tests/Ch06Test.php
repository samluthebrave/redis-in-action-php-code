<?php

namespace RedisInAction\Ch06;

use RedisInAction\Test\TestCase;

class Ch06Test extends TestCase
{
    protected function setUp()
    {
        parent::setUp();
    }

    protected function tearDown()
    {
        $this->conn->flushdb();

        parent::tearDown();
    }

    public function test_add_update_contact()
    {
        $conn = $this->conn;

        $conn->del('recent:user');
        self::pprint("Let's add a few contacts...");
        for ($i = 0; $i < 10; $i++) {
            add_update_contact(
                $conn, 'user', sprintf('contact-%d-%d', floor($i / 3), $i)
            );
        }
        self::pprint("Current recently contacted contacts");
        $contacts = $conn->lrange('recent:user', 0, -1);
        self::pprint($contacts);
        $this->assertTrue(count($contacts) >= 10);
        self::pprint();

        self::pprint("Let's pull one of the older ones up to the front");
        add_update_contact($conn, 'user', 'contact-1-4');
        $contacts = $conn->lrange('recent:user', 0, 2);
        self::pprint("New top-3 contacts:");
        self::pprint($contacts);
        $this->assertEquals($contacts[0], 'contact-1-4');
        self::pprint();

        self::pprint("Let's remove a contact...");
        self::pprint(remove_contact($conn, 'user', 'contact-2-6'));
        $contacts = $conn->lrange('recent:user', 0, -1);
        self::pprint("New contacts:");
        self::pprint($contacts);
        $this->assertTrue(count($contacts) >= 9);
        self::pprint();

        self::pprint("And let's finally autocomplete on");
        $all = $conn->lrange('recent:user', 0, -1);
        $contacts = fetch_autocomplete_list($conn, 'user', 'c');
        $this->assertTrue($all == $contacts);

        $equiv = array_filter(
            $all,
            function ($c) { return strpos($c, 'contact-2-') === 0; }
        );
        $contacts = fetch_autocomplete_list($conn, 'user', 'contact-2-');
        sort($equiv);
        sort($contacts);
        $this->assertEquals($equiv, $contacts);
        $conn->del('recent:user');
    }

    public function test_address_book_autocomplete()
    {
        $this->conn->del('members:test');

        self::pprint("the start/end range of 'abc' is:");
        self::pprint(find_prefix_range('abc'));
        self::pprint();

        self::pprint("Let's add a few people to the guild");
        foreach (['jeff', 'jenny', 'jack', 'jennifer'] as $name) {
            join_guild($this->conn, 'test', $name);
        }
        self::pprint();
        self::pprint("now let's try to find users with names starting with 'je':");
        $r = autocomplete_on_prefix($this->conn, 'test', 'je');
        self::pprint($r);
        $this->assertTrue(count($r) == 3);

        self::pprint("jeff just left to join a different guild...");
        leave_guild($this->conn, 'test', 'jeff');
        $r = autocomplete_on_prefix($this->conn, 'test', 'je');
        self::pprint($r);
        $this->assertTrue(count($r) == 2);

        $this->conn->del('members:test');
    }

    public function test_distributed_locking()
    {
        $this->conn->del('lock:testlock');

        self::pprint("Getting an initial lock...");
        $this->assertNotFalse(acquire_lock_with_timeout($this->conn, 'testlock', 1, 1));
        self::pprint("Got it!");

        self::pprint("Trying to get it again without releasing the first one...");
        $this->assertFalse(acquire_lock_with_timeout($this->conn, 'testlock', .01, 1));
        self::pprint("Failed to get it!");
        self::pprint();

        self::pprint("Waiting for the lock to timeout...");
        sleep(2);
        self::pprint("Getting the lock again...");
        $r = acquire_lock_with_timeout($this->conn, 'testlock', 1, 1);
        $this->assertNotFalse($r);
        self::pprint("Got it!");

        self::pprint("Releasing the lock...");
        $this->assertTrue(release_lock($this->conn, 'testlock', $r));
        self::pprint("Released it...");
        self::pprint();

        self::pprint("Acquiring it again...");
        $this->assertNotFalse(acquire_lock_with_timeout($this->conn, 'testlock', 1, 1));
        self::pprint("Got it!");

        $this->conn->del('lock:testlock');
    }
}