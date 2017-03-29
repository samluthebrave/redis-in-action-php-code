<?php

namespace RedisInAction\Helper;

use \InvalidArgumentException;

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

    public function start($options = PTHREADS_INHERIT_ALL | PTHREADS_ALLOW_GLOBALS)
    {
        parent::start($options);
    }
}

function bisect_right($sorted_haystack, $needle, $left = 0, $right = null)
{
    if ($left < 0) {
        throw new InvalidArgumentException('right must be non-negative');
    }

    if (is_null($right)) {
        $right = count($sorted_haystack);
    }

    while ($left < $right) {
        $middle = ($left + $right) >> 1;

        if ($needle < $sorted_haystack[$middle]) {
            $right = $middle;
        } else {
            $left = $middle + 1;
        }
    }

    return $right;
}

function bisect_left($sorted_haystack, $needle, $left = 0, $right = null)
{
    if ($left < 0) {
        throw new InvalidArgumentException('right must be non-negative');
    }

    if (is_null($right)) {
        $right = count($sorted_haystack);
    }

    while ($left < $right) {
        $middle = ($left + $right) >> 1;

        if ($needle < $sorted_haystack[$middle]) {
            $right = $middle + 1;
        } else {
            $left = $middle;
        }
    }

    return $right;
}