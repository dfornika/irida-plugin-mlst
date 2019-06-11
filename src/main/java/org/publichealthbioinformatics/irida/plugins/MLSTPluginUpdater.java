package org.publichealthbioinformatics.irida.plugins;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import ca.corefacility.bioinformatics.irida.exceptions.IridaWorkflowNotFoundException;
import ca.corefacility.bioinformatics.irida.exceptions.PostProcessingException;
import ca.corefacility.bioinformatics.irida.model.sample.MetadataTemplateField;
import ca.corefacility.bioinformatics.irida.model.sample.Sample;
import ca.corefacility.bioinformatics.irida.model.sample.metadata.MetadataEntry;
import ca.corefacility.bioinformatics.irida.model.sample.metadata.PipelineProvidedMetadataEntry;
import ca.corefacility.bioinformatics.irida.model.workflow.IridaWorkflow;
import ca.corefacility.bioinformatics.irida.model.workflow.analysis.AnalysisOutputFile;
import ca.corefacility.bioinformatics.irida.model.workflow.analysis.type.AnalysisType;
import ca.corefacility.bioinformatics.irida.model.workflow.submission.AnalysisSubmission;
import ca.corefacility.bioinformatics.irida.pipeline.results.updater.AnalysisSampleUpdater;
import ca.corefacility.bioinformatics.irida.service.sample.MetadataTemplateService;
import ca.corefacility.bioinformatics.irida.service.sample.SampleService;
import ca.corefacility.bioinformatics.irida.service.workflow.IridaWorkflowsService;

/**
 * This implements a class used to perform post-processing on the analysis
 * pipeline results to extract information to write into the IRIDA metadata
 * tables. Please see
 * <https://github.com/phac-nml/irida/blob/development/src/main/java/ca/corefacility/bioinformatics/irida/pipeline/results/AnalysisSampleUpdater.java>
 * or the README.md file in this project for more details.
 */
public class MLSTPluginUpdater implements AnalysisSampleUpdater {

    private final MetadataTemplateService metadataTemplateService;
    private final SampleService sampleService;
    private final IridaWorkflowsService iridaWorkflowsService;

    /**
     * Builds a new {@link MLSTPluginUpdater} with the given services.
     *
     * @param metadataTemplateService The metadata template service.
     * @param sampleService           The sample service.
     * @param iridaWorkflowsService   The irida workflows service.
     */
    public MLSTPluginUpdater(MetadataTemplateService metadataTemplateService, SampleService sampleService,
                             IridaWorkflowsService iridaWorkflowsService) {
        this.metadataTemplateService = metadataTemplateService;
        this.sampleService = sampleService;
        this.iridaWorkflowsService = iridaWorkflowsService;
    }

    /**
     * Code to perform the actual update of the {@link Sample}s passed in the
     * collection.
     *
     * @param samples  A collection of {@link Sample}s that were passed to this
     *                 pipeline.
     * @param analysisSubmission The {@link AnalysisSubmission} object corresponding to this
     *                 analysis submission.
     */
    @Override
    public void update(Collection<Sample> samples, AnalysisSubmission analysisSubmission) throws PostProcessingException {
        if (samples == null) {
            throw new IllegalArgumentException("samples is null");
        } else if (analysisSubmission == null) {
            throw new IllegalArgumentException("analysis is null");
        } else if (samples.size() != 1) {
            // In this particular pipeline, only one sample should be run at a time so I
            // verify that the collection of samples I get has only 1 sample
            throw new IllegalArgumentException(
                    "samples size=" + samples.size() + " is not 1 for analysisSubmission=" + analysisSubmission.getId());
        }

        // extract the 1 and only sample (if more than 1, would have thrown an exception
        // above)
        final Sample sample = samples.iterator().next();

        // extracts paths to the analysis result files
        AnalysisOutputFile mlstAnalysisFile = analysisSubmission.getAnalysis().getAnalysisOutputFile("mlst");
        Path mlstFile = mlstAnalysisFile.getFile();

        try {
            Map<String, MetadataEntry> metadataEntries = new HashMap<>();

            // get information about the workflow (e.g., version and name)
            IridaWorkflow iridaWorkflow = iridaWorkflowsService.getIridaWorkflow(analysisSubmission.getWorkflowId());
            String workflowVersion = iridaWorkflow.getWorkflowDescription().getVersion();
            String workflowName = iridaWorkflow.getWorkflowDescription().getName();

            // gets information from the "mlst.tsv" output file and constructs metadata
            // objects
            Map<String, String> mlstValues = parseMlstFile(mlstFile);
            for (String mlstField : mlstValues.keySet()) {
                final String mlstValue = mlstValues.get(mlstField);

                PipelineProvidedMetadataEntry mlstEntry = new PipelineProvidedMetadataEntry(mlstValue, "text",
                        analysisSubmission);

                // key will be string like 'mlst/0.1.0/scheme'
                String key = workflowName.toLowerCase() + "/" + mlstField;
                metadataEntries.put(key, mlstEntry);
            }

            Map<MetadataTemplateField, MetadataEntry> metadataMap = metadataTemplateService
                    .getMetadataMap(metadataEntries);

            // merges with existing sample metadata
            sample.mergeMetadata(metadataMap);

            // does an update of the sample metadata
            sampleService.updateFields(sample.getId(), ImmutableMap.of("metadata", sample.getMetadata()));
        } catch (IOException e) {
            throw new PostProcessingException("Error parsing hash file", e);
        } catch (IridaWorkflowNotFoundException e) {
            throw new PostProcessingException("Could not find workflow for id=" + analysisSubmission.getWorkflowId(), e);
        }
    }

    /**
     * Parses out values from the MLST output file into a {@link Map} linking 'mlstField' to
     * 'mlstValue'.
     *
     * @param mlstFile The {@link Path} to the file containing the hash values from
     *                 the pipeline. This file should contain contents like:
     *
     *                 <pre>
     * NM003   neisseria  4821  abcZ(222)  adk(3)  aroE(58)  fumC(275)  gdh(30)  pdhC(5)  pgm(255)
     *                 </pre>
     *
     * @return A {@link Map} linking 'mlstField' to 'mlstValue'.
     * @throws IOException             If there was an error reading the file.
     * @throws PostProcessingException If there was an error parsing the file.
     */
    private Map<String, String> parseMlstFile(Path mlstFile) throws IOException, PostProcessingException {
        Map<String, String> mlstResults = new HashMap<>();

        BufferedReader mlstReader = new BufferedReader(new FileReader(mlstFile.toFile()));

        try {

            String[] mlstFields = {
                    "sample_id",
                    "scheme",
                    "sequence_type",
            };

            String mlstValuesLine = mlstReader.readLine();
            String[] mlstValues = mlstValuesLine.split("\t");

            // Start index at 1, skip sample_id from mlstFile
            for (int i = 1; i < mlstFields.length; i++) {
                mlstResults.put(mlstFields[i], mlstValues[i]);
            }

            if (mlstReader.readLine() != null) {
                throw new PostProcessingException("Too many lines in file " + mlstFile);
            }
        } finally {
            // make sure to close, even in cases where an exception is thrown
            mlstReader.close();
        }

        return mlstResults;
    }

    /**
     * The {@link AnalysisType} this {@link AnalysisSampleUpdater} corresponds to.
     *
     * @return The {@link AnalysisType} this {@link AnalysisSampleUpdater}
     *         corresponds to.
     */
    @Override
    public AnalysisType getAnalysisType() {
        return MLSTPlugin.MLST;
    }
}
