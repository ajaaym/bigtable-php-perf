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
use Google\Cloud\Bigtable\Exception\BigtableDataOperationException;
use Google\Cloud\Bigtable\RowMutation;
use Google\Cloud\Bigtable\Performance\PcntlThread;
use \Thread;

class LoadData extends PcntlThread
{
    private $dataClient;
	private $randomValues = [];
    private $randomTotal = 1000;
    private $rowKeyPrefix;
    private $columnFamily;
    private $batchSize;
    private $start;
    private $end;
    private $hdrData;
    private $success = 0;
    private $failure = 0;
    private $projectId;
    private $instanceId;
    private $tableId;
    private $options;
    private $autoLoaderPath;
    private $keyFilePath;

    public function __construct($projectId, $instanceId, $tableId, $rowKeyPrefix, $columnFamily, $batchSize, $start, $end, $keyFilePath, $autoLoaderPath)
    {
        parent::__construct();
        $this->autoLoaderPath = $autoLoaderPath;
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
        // require_once $this->autoLoaderPath;
        $dataClient = new DataClient($this->instanceId, $this->tableId, [
            'projectId' => $this->projectId,
            'credentials' => $this->keyFilePath
        ]);
        $totalRows = $this->end - $this->start;
        echo "starting data load from $this->start to $this->end totalRows $totalRows \n";
        $interations = $totalRows/$this->batchSize;
        $hdr = hdr_init(1, 3600000, 3);
        $processStartTime = round(microtime(true)*1000);
        $numberOfRows=$this->start;
        for ($k = 0; $k < $interations && $numberOfRows < $this->end; $k++) {
            echo "starting iteration $k\n";
            $j = 0; 
            $rowMutations = [];
            for (;$j < $this->batchSize && $numberOfRows < $this->end; $j++) {
                for ($j = 0; $j < $this->batchSize; $j++) {
                    $rowKey        = sprintf($this->rowKeyPrefix.'%07d', $numberOfRows);
                    $rowMutation = new RowMutation($rowKey);
                    for ($i = 0; $i < 10; $i++) {
                        $value = $this->randomValues[mt_rand(1, $this->randomTotal)];
                        $rowMutation->upsert($this->columnFamily, 'field'.$i, $value);
                    }
                    $rowMutations[] = $rowMutation;
                    $numberOfRows++;
                }
                $startTime    = round(microtime(true)*1000);
            }
            try {
                echo "sending request\n";
                $dataClient->mutateRows($rowMutations);
                $this->success+= $j-1;
            } catch (BigtableDataOperationException $gdoe) {
                $this->failure+= count($dgoe->getMetadata());
            }
            $endTime = round(microtime(true)*1000)-$startTime;
            hdr_record_value($hdr, $endTime);
        }
        $timeElapsedSeconds= round(microtime(true)*1000)  - $processStartTime;
        $this->hdrData = serialize(hdr_export($hdr));
        echo "done data load from $this->start to $this->end totalRows $totalRows  success $this->success failure $this->failure totalTime $timeElapsedSeconds (milli sec)\n";
        $result = [];
        $result[] = $this->success;
        $result[] = $this->failure;
        $result[] = $this->hdrData;
        return $result;
    }

    public function getHdrData()
    {
        return unserialize($this->result[2]);
    }

    public function getSuccess() {
        return $this->result[0];
    }

    public function getFailure() {
        return $this->result[1];
    }
}
