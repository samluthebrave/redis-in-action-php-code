<?php

namespace RedisInAction\Helper;

class Threading extends \Thread
{
    private $globals = [];

    private $function;
    private $args = [];

    private $autoloader;

    public function __construct(callable $function, array $args = [], array $default_globals = [])
    {
        $this->function = $function;
        $this->args = $args;

        if (!empty($default_globals)) {
            foreach ($default_globals as $key => $value) {
                $this->setGlobal($key, $value);
            }
        }

        $this->autoloader = require 'vendor/autoload.php';
    }

    public function setGlobal($key, $value)
    {
        // why use `array_merge`?
        // see https://blog.madewithlove.be/post/thread-carefully/#data-corruption
        $this->globals = array_merge($this->globals, [$key => $value]);
    }

    public function getGlobal($key, $default_value = null)
    {
        $globals = $this->globals;

        return isset($globals[$key]) ? $globals[$key] : $default_value;
    }

    public function run()
    {
        // @see https://github.com/composer/composer/issues/5482
        $this->autoloader->register();

        $args = array_merge($this->args, [$this]);

        call_user_func_array($this->function, $args);
    }
}

function bisect_right($sorted_array, $key)
{
    end($sorted_array);
    $right = key($sorted_array);

    reset($sorted_array);
    $left = key($sorted_array);

    if ($key < $sorted_array[$left]) {
        return 0;
    } elseif ($key >= $sorted_array[$right]) {
        return count($sorted_array);
    }

    while ($right - $left > 1) {
        $middle = intval(($left + $right) / 2);
        if ($key >= $sorted_array[$middle]) {
            $left = $middle;
        } else {
            $right = $middle;
        }
    }

    return $right;
}