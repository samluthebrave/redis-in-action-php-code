<?php

class Ch01Test extends AbstractTestCase
{
    public function test_article_functionality()
    {
        $conn = $this->conn;

        $article_id = strval(post_article($conn, 'username', 'A title', 'http://www.google.com'));
        self::pprint("We posted a new article with id: " . $article_id);
        self::pprint();
        $this->assertNotEmpty($article_id);

        self::pprint("Its HASH looks like:");
        $r = $conn->hgetall('article:' . $article_id);
        self::pprint($r);
        self::pprint();
        $this->assertNotEmpty($r);

        article_vote($conn, 'other_user', 'article:' . $article_id);
        self::pprint("We voted for the article, it now has votes:");
        $v = intval($conn->hget('article:' . $article_id, 'votes'));
        self::pprint($v);
        self::pprint();
        $this->assertTrue($v > 1);

        self::pprint("The currently highest-scoring articles are:");
        $articles = get_articles($conn, 1);
        self::pprint($articles);
        self::pprint();

        $this->assertTrue(count($articles) >= 1);

        add_remove_groups($conn, $article_id, ['new-group']);
        self::pprint("We added the article to a new group, other articles include:");
        $articles = get_group_articles($conn, 'new-group', 1);
        self::pprint($articles);
        self::pprint();
        $this->assertTrue(count($articles) >= 1);

        $to_del = array_merge(
            $conn->keys('time:*'), $conn->keys('voted:*'), $conn->keys('score:*'),
            $conn->keys('article:*'), $conn->keys('group:*')
        );
        if ($to_del) {
            $conn->del($to_del);
        }
    }
}
