<?php

namespace RedisInAction\Ch08;

use RedisInAction\Test\TestCase;

class Ch08Test extends TestCase
{
    protected function setUp()
    {
        parent::setUp();

        $this->conn->flushDb();
    }

    protected function tearDown()
    {
//        $this->conn->flushDb();

        parent::tearDown();
    }

    public function test_create_user_and_status()
    {
        $this->assertEquals(
            1, create_user($this->conn, 'TestUser', 'Test User')
        );
        $this->assertNull(create_user($this->conn, 'TestUser', 'Test User2'));

        $this->assertEquals(
            1, create_status($this->conn, 1, "This is a new status message")
        );
        $this->assertEquals('1', $this->conn->hget('user:1', 'posts'));
    }

    public function test_follow_unfollow_user()
    {
        $this->assertEquals(create_user($this->conn, 'TestUser', 'Test User'), 1);
        $this->assertEquals(create_user($this->conn, 'TestUser2', 'Test User2'), 2);

        $this->assertTrue(follow_user($this->conn, 1, 2));
        $this->assertEquals(1, $this->conn->zcard('followers:2'));
        $this->assertEquals(0, $this->conn->zcard('followers:1'));
        $this->assertEquals(1, $this->conn->zcard('following:1'));
        $this->assertEquals(0, $this->conn->zcard('following:2'));
        $this->assertEquals('1', $this->conn->hget('user:1', 'following'));
        $this->assertEquals('0', $this->conn->hget('user:2', 'following'));
        $this->assertEquals('0', $this->conn->hget('user:1', 'followers'));
        $this->assertEquals('1', $this->conn->hget('user:2', 'followers'));

        $this->assertEquals(null, unfollow_user($this->conn, 2, 1));
        $this->assertEquals(true, unfollow_user($this->conn, 1, 2));
        $this->assertEquals(0, $this->conn->zcard('followers:2'));
        $this->assertEquals(0, $this->conn->zcard('followers:1'));
        $this->assertEquals(0, $this->conn->zcard('following:1'));
        $this->assertEquals(0, $this->conn->zcard('following:2'));
        $this->assertEquals('0', $this->conn->hget('user:1', 'following'));
        $this->assertEquals('0', $this->conn->hget('user:2', 'following'));
        $this->assertEquals('0', $this->conn->hget('user:1', 'followers'));
        $this->assertEquals('0', $this->conn->hget('user:2', 'followers'));
    }

    public function test_syndicate_status()
    {
        $this->assertEquals(1, create_user($this->conn, 'TestUser', 'Test User'));
        $this->assertEquals(2, create_user($this->conn, 'TestUser2', 'Test User2'));

        $this->assertTrue(follow_user($this->conn, 1, 2));
        $this->assertEquals(1, $this->conn->zcard('followers:2'));
        $this->assertEquals('1', $this->conn->hget('user:1', 'following'));
        $this->assertEquals(
            1, post_status($this->conn, 2, 'this is some message content')
        );
        $this->assertEquals(1, count(get_status_messages($this->conn, 1)));

        for ($i = 3; $i < 11; $i++) {
            $this->assertEquals(
                $i,
                create_user($this->conn, sprintf('TestUser%s', $i), sprintf('Test User%s', $i))
            );
            follow_user($this->conn, $i, 2);
        }

        global $POSTS_PER_PASS;
        $POSTS_PER_PASS = 5;

        $this->assertEquals(
            2,
            post_status($this->conn, 2, 'this is some other message content')
        );

        usleep(100000);
        $this->assertEquals(2, count(get_status_messages($this->conn, 9)));

        return;
        $this->assertTrue(unfollow_user($this->conn, 1, 2));
        $this->assertEquals(0, count(get_status_messages($this->conn, 1)));
    }

    public function test_refill_timeline()
    {
        $this->assertEquals(1, create_user($this->conn, 'TestUser', 'Test User'));
        $this->assertEquals(2, create_user($this->conn, 'TestUser2', 'Test User2'));
        $this->assertEquals(3, create_user($this->conn, 'TestUser3', 'Test User3'));

        $this->assertTrue(follow_user($this->conn, 1, 2));
        $this->assertTrue(follow_user($this->conn, 1, 3));

        global $HOME_TIMELINE_SIZE;

        $HOME_TIMELINE_SIZE = 5;

        for ($i = 0; $i < 10; $i++) {
            $this->assertNotNull(post_status($this->conn, 2, 'message'));
            $this->assertNotNull(post_status($this->conn, 3, 'message'));
            usleep(50000);
        }

        $this->assertEquals(5, count(get_status_messages($this->conn, 1)));
        $this->assertTrue(unfollow_user($this->conn, 1, 2));
        $this->assertTrue(count(get_status_messages($this->conn, 1)) < 5);


        refill_timeline($this->conn, 'following:1', 'home:1');

        $messages = get_status_messages($this->conn, 1);
        $this->assertEquals(5, count($messages));
        foreach ($messages as $msg) {
            $this->assertEquals('3', $msg['uid']);
        }

        delete_status($this->conn, '3', end($messages)['id']);
        $this->assertEquals(4, count(get_status_messages($this->conn, 1)));
        $this->assertEquals(5, $this->conn->zcard('home:1'));

        clean_timelines($this->conn, '3', end($messages)['id']);
        $this->assertEquals(4, $this->conn->zcard('home:1'));
    }
}