/*
CREATE TABLE `batch_history` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `year_season` char(5) DEFAULT NULL COMMENT '民國年+季度 ex.114S1',
  `csv_path` varchar(1023) DEFAULT NULL,
  `create_time` timestamp NULL DEFAULT current_timestamp(),
  `row_count` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
*/