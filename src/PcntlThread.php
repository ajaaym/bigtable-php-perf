<?php
/**
 * Copyright 2018, Google LLC All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Google\Cloud\Bigtable\Performance;

abstract class PcntlThread
{
    const SHARED_MEMORY_SIZE = 4096;

    private $sharedMemoryName;
    private $childPid = -1;
    protected $result;
    private $sharedMemory;

    public function __construct($name = "") {
        $this->sharedMemoryName = $name . substr(str_shuffle(str_repeat($x = '0123456789', ceil(5/strlen($x)))), 1, 5);
    }

    public function start()
    {
        echo "creating shared memory ".$this->sharedMemoryName." \n";
        $this->sharedMemory = shmop_open($this->sharedMemoryName, "c", 0644, self::SHARED_MEMORY_SIZE);
        $pid = pcntl_fork();
        if ($pid == -1) {
            throw new \Exception('Error in creating child process');
        } else if ($pid) {
            $this->childPid = $pid;
        } else {
            $result = $this->run();
            echo "completed the thread \n";
            shmop_write($this->sharedMemory, serialize($result), 0);
            exit();
        }
    }

    public function join() {
        echo "waiting for pid $this->childPid \n";
        pcntl_waitpid($this->childPid, $status);
        echo "completed pid $this->childPid \n";
        $result = shmop_read($this->sharedMemory, 0, 0);
        shmop_delete($this->sharedMemory);
        shmop_close($this->sharedMemory);
        $this->result = deserialize($result);
    }
    public abstract function run();
}
