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

class PerformanceTest
{
	private $dataClient;
	private $randomValues;
    private $randomTotal = 1000;
    private $rowKeyPrefix;
    private $columnFamily;
    private $totalRow;
    private $batchSize;
    private $timeout;

	function __construct($instanceId, $tableId, $rowKeyPrefix, $columnFamily, $totalRow, $batchSize, $timeout) {
        $this->dataClient = new DataClient($instanceId, $tableId);
        $this->rowKeyPrefix = $rowKeyPrefix;
        $this->columnFamily = $columnFamily;
        $this->totalRow = $totalRow;
        $this->batchSize = $batchSize;
        $this->timeout = $timeout;
        if ($totalRow < $batchSize) {
			throw new ValidationException('Please set total row (total_row) >= '.$batchSize);
		}
		$length              = 100;
		for ($i = 1; $i <= $this->randomTotal; $i++) {
			$this->randomValues[$i] = substr(str_shuffle(str_repeat($x = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', ceil($length/strlen($x)))), 1, $length);
		}
	}
	/**
	 * loadRecord for mutateRows in table
	 *
	 * @param string $rowKeyPrefix   ex. perf
	 * @param string $columnFamily	column family name
	 * @param array  optionalArgs{
	 *     @integer $total_row
	 *     @integer $batch_size
	 *     @integer $timeoutMillis
	 *
	 * @return array
	 */
	public function loadRecord() {
		$interations = $this->totalRow/$this->batchSize;
		$hdr = hdr_init(1, 3600000, 3);
		$index            = 0;
		$success          = 0;
		$failure          = 0;
		$processStartTime = round(microtime(true)*1000);
		for ($k = 0; $k < $interations; $k++) {
			$rowMutations = [];
			for ($j = 0; $j < $this->batchSize; $j++) {
                $rowKey        = sprintf($this->rowKeyPrefix.'%07d', $index);
                $rowMutation = new RowMutation($rowKey);
				for ($i = 0; $i < 10; $i++) {
                    
					$value = $this->randomValues[mt_rand(1, $this->randomTotal)];
                    $rowMutation->upsert($this->columnFamily, 'field'.$i, $value);
                }
                $rowMutations[] = $rowMutation;
                $index++;
			}
			$startTime    = round(microtime(true)*1000);
			$serverStream = $this->dataClient->mutateRows($rowMutations);
			$current = $ServerStream->readAll()->current();
            $Entries = $current->getEntries();
            foreach ($serverStream->readAll() as $current) {
                $entries = $current->getEntries();
                foreach ($entries->getIterator() as $Iterator) {
                    $status = $Iterator->getStatus();
                    $code   = $status->getCode();
                    if ($code == 0) {
                        $success++;
                    } else if ($code == 1) {
                        $failure++;
                    }
                }
            }
			$endTime = round(microtime(true)*1000)-$startTime;
			hdr_record_value($hdr, $endTime);
		}
		$timeElapsedSeconds= round(microtime(true)*1000)  - $processStartTime;
		echo "\nTotal time takes for loading rows is $time_elapsed_secs (milli sec) totalrow $totalRow";
		$min           = hdr_min($hdr);
		$max           = hdr_max($hdr);
		$total         = $index;
		$totalSec      = $timeElapsedSeconds / 1000;
		$throughput    = round($total/$totalSec, 4);
		$statisticData = [
			'operation_name'     => 'Data Load',
			'run_time'           => $timeElapsedSeconds,
			'mix_latency'        => $max/100,
			'min_latency'        => $min/100,
			'oprations'          => $total,
			'throughput'         => $throughput,
			'p50_latency'        => hdr_value_at_percentile($hdr, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr, 99.99),
			'success_operations' => $success,
			'failed_operations'  => $failure
		];
		return $statisticData;
	}
	/**
	 * random read write row
	 *
	 *
	 * @return array
	 */
	public function randomReadWrite() {
        $readRowsSuccess = 0;
        $readRowsFailure = 0;
        $writeRowsSuccess = 0;
        $writeRowsFailure = 0;
		$hdr_read  = hdr_init(1, 3600000, 3);
		$hdr_write = hdr_init(1, 3600000, 3);
		$operationStart            = round(microtime(true)*1000);
		$readOprationsTotalTime  = 0;
		$writeOprationsTotalTime = 0;
		$startTime = date("h:i:s");
		echo "\nRandom read write start Time $startTime";
		$currentTimestamp = new DateTime($startTime);
		$endTime      = date(" h:i:s", time()+$this->timeout);
		$endTimestamp = new DateTime($endTime);
		echo "\nRandom read write process will terminate after $endTime";
		echo "\nPlease wait ...";
		$i = 0;
		while ($currentTimestamp < $endTimestamp) {
			$random       = mt_rand(0, $this->totalRows);
			$randomRowKey = sprintf($this->rowKeyPref.'%07d', $random);
			$start        = round(microtime(true)*1000);
			if ($i%2 == 0) {
                try {
                    iterator_to_array($this->dataTable->readRows(['rowKeys' => $randomRowKey]));
                    $readRowsSuccess++;
                } catch(\Exception $e ) {
                    $readRowsFailure++;
                }
				$timeElapsedSecs = round(microtime(true)*1000)-$start;
				$readOprationsTotalTime += $timeElapsedSecs;
				hdr_record_value($hdr_read, $timeElapsedSecs);
			} else {
                $rowMutation = new RowMutation($randomRowKey);
                for ($i=0; $i<10; $i++) {
                    $value = $this->randomValues[mt_rand(1, $this->randomTotal)];
                    $rowMutation->upsert($this->columnFamily, 'field'.$i, $value);
                }
                try {
                    $response = iterator_to_array($this->dataClient->mutateRow([$rowMutation]));
                    $writeRowsSuccess++;
                } catch(\Exception $e) {
                    $writeRowsFailure++;
                }
				$timeElapsedSecs = round(microtime(true)*1000)-$start;
				$writeOprationsTotalTime += $timeElapsedSecs;
				hdr_record_value($hdr_write, $timeElapsedSecs);
			}
			$i++;
			$currentTimestamp = new DateTime(date("h:i:s"));
		}
		echo "\nRandom read write rows operation complete\n";
		$totalRuntime = round(microtime(true)*1000)-$operationStart;
		//Read operations
		$minRead       = hdr_min($hdr_read);
		$maxRead       = hdr_max($hdr_read);
		$totalRead     = count($readRowsSuccess)+count($readRowsFailure);
		$totalReadTimeSec = $readOprationsTotalTime/1000;
		$readThroughput = round($totalRead/$totalReadTimeSec, 4);
		$readOperations = [
			'operation_name'     => 'Random Read',
			'run_time'           => $readOprationsTotalTime,
			'mix_latency'        => $maxRead/100,
			'min_latency'        => $minRead/100,
			'oprations'          => $totalRead,
			'throughput'         => $readThroughput,
			'p50_latency'        => hdr_value_at_percentile($hdr_read, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr_read, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr_read, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr_read, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr_read, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr_read, 99.99),
			'success_operations' => count($readRowsSuccess),
			'failed_operations'  => count($readRowsFailure)
		];
		//Write Operations
		$min_write       = hdr_min($hdr_write);
		$max_write       = hdr_max($hdr_write);
		$total_write     = count($writeRowsSuccess)+count($writeRowsFailure);
		$totalWriteTimeSec = $writeOprationsTotalTime/1000;
		$writeThroughput = round($total_write/$totalWriteTimeSec, 4);
		$writeOperations = [
			'operation_name'     => 'Random Write',
			'run_time'           => $writeOprationsTotalTime,
			'mix_latency'        => $max_write/100,
			'min_latency'        => $min_write/100,
			'oprations'          => $total_write,
			'throughput'         => $writeThroughput,
			'p50_latency'        => hdr_value_at_percentile($hdr_write, 50),
			'p75_latency'        => hdr_value_at_percentile($hdr_write, 75),
			'p90_latency'        => hdr_value_at_percentile($hdr_write, 90),
			'p95_latency'        => hdr_value_at_percentile($hdr_write, 95),
			'p99_latency'        => hdr_value_at_percentile($hdr_write, 99),
			'p99.99_latency'     => hdr_value_at_percentile($hdr_write, 99.99),
			'success_operations' => count($writeRowsSuccess),
			'failed_operations'  => count($writeRowsFailure)
		];
		return (['readOperations' => $readOperations, 'writeOperations' => $writeOperations]);
	}
}