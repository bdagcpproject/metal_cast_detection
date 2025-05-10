


-- Table 2: Confidence Score Metrics (related to results_metrics)
CREATE OR REPLACE TABLE `cast-defect-detection.cast_defect_detection.confidencescore_metrics` (
  id STRING,
    insert_datetime DATETIME,
  confidence_score_min FLOAT64,
  confidence_score_med FLOAT64,
  confidence_score_mean FLOAT64,
  confidence_score_max FLOAT64
);

-- Table 3: Inference Time Metrics (related to results_metrics)
CREATE OR REPLACE TABLE `cast-defect-detection.cast_defect_detection.inference_metrics` (
  id STRING,
    insert_datetime DATETIME,
  inference_time_min FLOAT64,
  inference_time_med FLOAT64,
  inference_time_mean FLOAT64,
  inference_time_max FLOAT64
);

-- Table 4: Prediction Class Frequencies (related to results_metrics)
CREATE OR REPLACE TABLE `cast-defect-detection.cast_defect_detection.prediction_class_metrics` (
  id STRING,
    insert_datetime DATETIME,
  pred_class_pass_freq INT64,
  pred_class_fail_freq INT64
);



ALTER TABLE `cast-defect-detection.cast_defect_detection.inference_metrics`
ADD COLUMN aggregation_start DATETIME,
ADD COLUMN aggregation_end DATETIME;

ALTER TABLE `cast-defect-detection.cast_defect_detection.confidencescore_metrics`
ADD COLUMN aggregation_start DATETIME,
ADD COLUMN aggregation_end DATETIME;

ALTER TABLE `cast-defect-detection.cast_defect_detection.prediction_class_metrics`
ADD COLUMN aggregation_start DATETIME,
ADD COLUMN aggregation_end DATETIME;


TRUNCATE TABLE `cast-defect-detection.cast_defect_detection.prediction_class_metrics`
TRUNCATE TABLE `cast-defect-detection.cast_defect_detection.confidencescore_metrics`
TRUNCATE TABLE `cast-defect-detection.cast_defect_detection.inference_metrics`


CREATE OR REPLACE TABLE `cast-defect-detection.cast_defect_detection.comments` (
  result_id STRING,
  comment_text STRING,
  comment_datetime DATETIME
);
