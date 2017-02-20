<?php

use RedisInAction\Threading;
use Ramsey\Uuid\Uuid;

class Ch02Test extends AbstractTestCase
{
    protected function tearDown()
    {
        $conn = $this->conn;
        $to_del = array_merge(
            $conn->keys('login:*'), $conn->keys('recent:*'), $conn->keys('viewed:*'),
            $conn->keys('cart:*'), $conn->keys('cache:*'), $conn->keys('delay:*'),
            $conn->keys('schedule:*'), $conn->keys('inv:*')
        );
        if ($to_del) {
            $this->conn->del($to_del);
        }
        unset($this->conn);
        global $QUIT, $LIMIT;
        $QUIT = false;
        $LIMIT = 10000000;
        self::pprint();
        self::pprint();
    }

    public function test_login_cookies()
    {
        $conn = $this->conn;
        global $QUIT, $LIMIT;
        $token = Uuid::uuid4()->toString();

        update_token($conn, $token, 'username', 'itemX');
        self::pprint("We just logged-in/updated token: " . $token);
        self::pprint("For user: " . 'username');
        self::pprint();

        self::pprint("What username do we get when we look-up that token?");
        $r = check_token($conn, $token);
        self::pprint($r);
        self::pprint();
        $this->assertNotEmpty($r);

        self::pprint("Let's drop the maximum number of cookies to 0 to clean them out");
        self::pprint("We will start a thread to do the cleaning, while we stop it later");

        $t = new Threading('clean_sessions', [$conn], ['QUIT' => $QUIT, 'LIMIT' => $LIMIT]);
        $t->setGlobal('LIMIT', 0);
        $t->start();
        sleep(1);
        $t->setGlobal('QUIT', true);
        sleep(2);
        if ($t->isRunning()) {
            throw new Exception("The clean sessions thread is still alive?!?");
        }

        $s = $conn->hlen('login:');
        self::pprint("The current number of sessions still available is: " . $s);
        $this->assertEmpty($s);
    }

    public function test_shopping_cart_cookies()
    {
        $conn = $this->conn;
        global $LIMIT, $QUIT;
        $token = Uuid::uuid4()->toString();

        self::pprint("We'll refresh our session...");
        update_token($conn, $token, 'username', 'itemX');
        self::pprint("And add an item to the shopping cart");
        add_to_cart($conn, $token, "itemY", 3);
        $r = $conn->hgetall('cart:' . $token);
        self::pprint("Our shopping cart currently has: ");
        self::pprint($r);

        $this->assertTrue(count($r) >= 1);

        self::pprint("Let's clean out our sessions and carts");
        $t = new Threading('clean_full_sessions', [$conn], ['QUIT' => $QUIT, 'LIMIT' => $LIMIT]);
        $t->setGlobal('LIMIT', 0);
        $t->start();
        sleep(1);
        $t->setGlobal('QUIT', true);
        sleep(2);
        if ($t->isRunning()) {
            throw new Exception("The clean sessions thread is still alive?!?");
        }

        $r = $conn->hgetall('cart:' + $token);
        self::pprint("Our shopping cart now contains:");
        self::pprint($r);

        $this->assertEmpty($r);
    }

    public function test_cache_request()
    {
        $conn = $this->conn;
        $token = Uuid::uuid4()->toString();

        $callback = function ($request) {
            return "content for " . $request;
        };

        update_token($conn, $token, 'username', 'itemX');
        $url = 'http://test.com/?item=itemX';
        self::pprint("We are going to cache a simple request against " . $url);
        $result = cache_request($conn, $url, $callback);
        self::pprint("We got initial content: " . var_export($result, true));
        self::pprint();

        $this->assertNotEmpty($result);

        self::pprint("To test that we've cached the request, we'll pass a bad callback");
        $result2 = cache_request($conn, $url, function () {});
        self::pprint("We ended up getting the same response! " . var_export($result2, true));
        
        $this->assertEquals($result, $result2);
        
        $this->assertFalse(can_cache($conn, 'http://test.com/'));
        $this->assertFalse(can_cache($conn, 'http://test.com/?item=itemX&_=1234536'));
    }

    public function test_cache_rows()
    {
        $conn = $this->conn;
        global $QUIT;

        self::pprint("First, let's schedule caching of itemX every 5 seconds");
        schedule_row_cache($conn, 'itemX', 5);
        self::pprint("Our schedule looks like:");
        $s = $conn->zrange('schedule:', 0, -1, ['WITHSCORES' => true]);
        self::pprint($s);
        $this->assertNotEmpty($s);

        self::pprint("We'll start a caching thread that will cache the data...");
        $t = new Threading('cache_rows', [$conn], ['QUIT' => $QUIT]);
        $t->start();

        sleep(1);
        self::pprint("Our cached data looks like:");
        $r = $conn->get('inv:itemX');
        self::pprint($r);
        $this->assertNotEmpty($r);
        self::pprint();
        self::pprint("We'll check again in 5 seconds...");
        sleep(5);
        self::pprint("Notice that the data has changed...");
        $r2 = $conn->get('inv:itemX');
        self::pprint($r2);
        self::pprint();
        $this->assertNotEmpty($r2);
        $this->assertTrue($r != $r2);

        self::pprint("Let's force un-caching");
        schedule_row_cache($conn, 'itemX', -1);
        sleep(1);
        $r = $conn->get('inv:itemX');
        self::pprint("The cache was cleared?");
        self::pprint(!$r);
        $this->assertEmpty($r);

        $t->setGlobal('QUIT', true);
        sleep(2);
        if (true) {
            throw new Exception("The database caching thread is still alive?!?");
        }
    }
}