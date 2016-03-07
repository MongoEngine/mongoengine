<?php

/**
 * UnitTest Engine for MongoEngine
 */

class MongoEngineUnitTestEngine extends ArcanistUnitTestEngine {

  private function strStartsWith($haystack, $needle)
  {
    $length = strlen($needle);
    return (substr($haystack, 0, $length) === $needle);
  }

  private function strEndsWith($haystack, $needle)
  {
        $length = strlen($needle);
            $start  = $length * -1; //negative
            return (substr($haystack, $start) === $needle);
  }

  public function run() {
    $projectRoot = $this->getWorkingCopy()->getProjectRoot();

    exec('python ' . $projectRoot . '/scripts/arcanist/unit/run_tests.py ' .
         '--dirs ' . $projectRoot . '/tests', $output, $ret_val);

    return $this->buildResultFromOutput($output);
  }

  private function getInfoLine($output) {
    $size = count($output);
    return $output[$size - 3];
  }

  private function getDuration($output) {
    $time_line = $this->getInfoLine($output);
    $chunks = explode("in", $time_line);
    $time = $chunks[1];
    $duration = substr($time, 0, -1);
    return $duration;
  }

  private function extractStackTraces($output) {
    $list_of_traces = Array();
    $consuming = false;
    for ($i = 0; $i < count($output); $i++) {
      $line = $output[$i];

      if ($this->strStartsWith($line, "FAIL:")
          || $this->strStartsWith($line, "ERROR:")) {
        $trace_lines = Array();
        for($j = $i; $j < count($output); $j++) {
          $t_line = $output[$j];
          if ($t_line == "") {
            break;
          }
        $trace_lines[] = $t_line;
        }
        $i += count($trace_lines);
        $list_of_traces[] = implode("\n", $trace_lines);
      }
    }
    return $list_of_traces;
  }

  private function buildResultFromOutput($output) {

    $json_output = json_decode($output[0]);
    $results = Array();


    foreach($json_output as $test_info) {
        $test_result = new ArcanistUnitTestResult();
        $test_result->setName($test_info->name);
        $test_result->setUserData($test_info->output);
        $test_result->setDuration(floatval($test_info->test_duration));
        $test_result->setResult(ArcanistUnitTestResult::RESULT_PASS);
        if($test_info->return_code != 0) {
            $test_result->setResult(ArcanistUnitTestResult::RESULT_FAIL);
        }
        array_push($results, $test_result);
    }

    return $results;
  }
}
