package com.gonnect.beam.gameaction;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options supported by {@link GameActionPipeline}.
 */
public interface GameInfoPipelineOptions extends PipelineOptions {
  @Description("BigQuery Dataset to write tables to. Must already exist.")
  // @Validation.Required
  String getDataset();

  void setDataset(String value);

  @Description("Prefix for output files, either local path or cloud storage location.")
  @Default.String("output/")
  String getOutputPrefix();
  void setOutputPrefix(String value);
}
