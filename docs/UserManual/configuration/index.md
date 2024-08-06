# Configuration

## Configuration Section Structure
The configuration section is divided in four sections:  

- **Pipeline**: Main pipeline components to generate indicators from Mno Data.  
- **Optional**: Components that are optional to the pipeline execution. They enrich some data objects which may lead to quality improvements of the final results but are not essential to the pipeline.  
- **QualityWarnings**: Components that analyze quality metrics of data objects.  
- **SyntheticMnoData**: Components that are used to create synthetic Mno Data. Mainly used for testing purposes.  

## Configuration files used
The multimno application requires from multiple configuration files.  

- **One general configuration file** describing general parameters like file paths, logging, spark and 
common values for all components in the pipeline.  
- **A configuration file per each component of the pipeline** with configuration parameters exclusive to the component. Values defined in these files can override values defined in the general configuration file.