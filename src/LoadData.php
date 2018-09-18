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

use Google\Cloud\Bigtable\DataClient;
use Google\Cloud\Bigtable\RowMutation;

use \Thread;

class LoadData extends Thread
{
    private $dataClient;
	private $randomValues = [];
    private $randomTotal = 1000;
    private $rowKeyPrefix;
    private $columnFamily;
    private $batchSize;
    private $start;
    private $end;
    private $hdr;
    private $success = 0;
    private $failure = 0;
    private $projectId;
    private $instanceId;
    private $tableId;
    private $options;
    private $loader;
    private $keyFilePath;

    public function __construct($projectId, $instanceId, $tableId, $rowKeyPrefix, $columnFamily, $batchSize, $start, $end, $keyFilePath, $loader)
    {
        $this->loader = $loader;
        $this->options = [
            'projectId' => $projectId,
            'credentials' => $keyFilePath
        ];
        $this->keyFilePath = $keyFilePath;
        $this->projectId = $projectId;
        $this->instanceId = $instanceId;
        $this->tableId = $tableId;
        $this->rowKeyPrefix = $rowKeyPrefix;
        $this->columnFamily = $columnFamily;
        $this->batchSize = $batchSize;
        $this->start = $start;
        $this->end = $end;
        $length = 100;
		for ($i = 1; $i <= $this->randomTotal; $i++) {
			$this->randomValues[$i] = substr(str_shuffle(str_repeat($x = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length/strlen($x)))), 1, $length);
		}
    }

    public function run()
    {
        $this->loader->register();
        $dataClient = new DataClient($this->instanceId, $this->tableId, [
            'projectId' => $this->projectId,
            'credentials' => $this->keyFilePath
        ]);
        echo "starting data load from ".$this->start." to ".$this->end."\n";
        $totalRows = $this->end - $this->start;
        $interations = $totalRows/$this->batchSize;
        $this->hdr = hdr_init(1, 3600000, 3);
        $processStartTime = round(microtime(true)*1000);
        $numberOfRows=0;
        $rowMutations = [];
        for ($k = 0; $k < $interations; $k++) {
            for ($j = 0; $j < $this->batchSize && $numberOfRows < $totalRows; $j++) {
                for ($j = 0; $j < $this->batchSize; $j++) {
                    $rowKey        = sprintf($this->rowKeyPrefix.'%07d', $index);
                    $rowMutation = new RowMutation($rowKey);
                    for ($i = 0; $i < 10; $i++) {
                        $value = $this->randomValues[mt_rand(1, $this->randomTotal)];
                        $rowMutation->upsert($this->columnFamily, 'field'.$i, $value);
                    }
                    $rowMutations[] = $rowMutation;
                    $numberOfRows++;
                }
                echo "number of rows inserting: ".$numberOfRows."\n";
                $startTime    = round(microtime(true)*1000);
                $serverStream = $dataClient->mutateRows($rowMutations);
                foreach ($serverStream->readAll() as $current) {
                    $entries = $current->getEntries();
                    foreach ($entries->getIterator() as $Iterator) {
                        $status = $Iterator->getStatus();
                        $code   = $status->getCode();
                        if ($code == 0) {
                            $this->success++;
                        } else if ($code == 1) {
                            $this->failure++;
                        }
                    }
                }
                $endTime = round(microtime(true)*1000)-$startTime;
                hdr_record_value($hdr, $endTime);
            }
        }
        $timeElapsedSeconds= round(microtime(true)*1000)  - $processStartTime;
        echo "starting data load from ".$this->start." to ".$this->end."\n";
		echo "\nTotal time takes for loading rows is $time_elapsed_secs (milli sec) totalrow $totalRows";
    }

    public function getHdr()
    {
        return $this->hdr;
    }

    public function getSuccess() {
        return $this->success;
    }

    public function getFailure() {
        return $this->failure;
    }
}
