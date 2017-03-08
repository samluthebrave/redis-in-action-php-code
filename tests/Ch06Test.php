<?php

namespace RedisInAction\Ch06;

use RedisInAction\Helper\Threading;
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

    public function test_counting_semaphore()
    {
        $this->conn->del(['testsem', 'testsem:owner', 'testsem:counter']);

        self::pprint("Getting 3 initial semaphores with a limit of 3...");
        for ($i = 0; $i < 3; $i++) {
            $this->assertNotNull(acquire_fair_semaphore($this->conn, 'testsem', 3, 1));
        }
        self::pprint("Done!");

        self::pprint("Getting one more that should fail...");
        $this->assertNull(acquire_fair_semaphore($this->conn, 'testsem', 3, 1));
        self::pprint("Couldn't get it!");
        self::pprint();

        self::pprint("Lets's wait for some of them to time out");
        sleep(2);
        self::pprint("Can we get one?");
        $r = acquire_fair_semaphore($this->conn, 'testsem', 3, 1);
        $this->assertNotNull($r);
        self::pprint("Got one!");

        self::pprint("Let's release it...");
        $this->assertNotEquals(0, release_fair_semaphore($this->conn, 'testsem', $r));
        self::pprint("Released!");
        self::pprint();

        self::pprint("And let's make sure we can get 3 more!");
        for ($i = 0; $i < 3; $i++) {
            $this->assertNotNull(acquire_fair_semaphore($this->conn, 'testsem', 3, 1));
        }
        self::pprint("We got them!");

        $this->conn->del(['testsem', 'testsem:owner', 'testsem:counter']);
    }

    public function test_delayed_tasks()
    {
        $this->conn->del(['queue:tqueue', 'delayed:']);

        self::pprint("Let's start some regular and delayed tasks...");
        foreach ([0, .5, 0, 1.5] as $delay) {
            $this->assertNotEmpty(
                execute_later($this->conn, 'tqueue', 'testfn', [], $delay)
            );
        }
        $r = $this->conn->llen('queue:tqueue');
        self::pprint("How many non-delayed tasks are there (should be 2)? " . $r);
        $this->assertEquals(2, $r);
        self::pprint();
        
        self::pprint("Let's start up a thread to bring those delayed tasks back...");
        $t = new Threading(__NAMESPACE__ . '\poll_queue', [$this->conn], ['QUIT' => false]);
        $t->start();
        self::pprint("Started.");
        self::pprint("Let's wait for those tasks to be prepared...");
        sleep(2);
        $t->setGlobal('QUIT', true);
        $t->join();

        $r = $this->conn->llen('queue:tqueue');
        self::pprint("Waiting is over, how many tasks do we have (should be 4)? " . $r);
        $this->assertEquals(4, $r);

        $this->conn->del(['queue:tqueue', 'delayed:']);
    }

    public function test_multi_recipient_messaging()
    {
        $this->conn->del('ids:chat:', 'msgs:1', 'ids:1', 'seen:joe', 'seen:jeff', 'seen:jenny');

        self::pprint("Let's create a new chat session with some recipients...");
        $chat_id = create_chat($this->conn, 'joe', ['jeff', 'jenny'], 'message 1');

        self::pprint("Now let's send a few messages...");
        for ($i = 2; $i < 5; $i++) {
            send_message($this->conn, $chat_id, 'joe', sprintf('message %s', $i));
        }
        self::pprint();

        self::pprint("And let's get the messages that are waiting for jeff and jenny...");
        $r1 = fetch_pending_messages($this->conn, 'jeff');
        $r2 = fetch_pending_messages($this->conn, 'jenny');
        self::pprint("They are the same? " . json_encode($r1 == $r2));
        $this->assertEquals($r1, $r2);
        self::pprint("Those messages are:");
        self::pprint($r1);

        $this->conn->del('ids:chat:', 'msgs:1', 'ids:1', 'seen:joe', 'seen:jeff', 'seen:jenny');
    }
}
