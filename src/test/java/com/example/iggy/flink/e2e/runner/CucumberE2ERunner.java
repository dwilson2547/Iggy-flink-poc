package com.example.iggy.flink.e2e.runner;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;
import static io.cucumber.junit.platform.engine.Constants.PLUGIN_PROPERTY_NAME;

/**
 * Cucumber E2E test runner.
 *
 * <p>Runs all feature files under {@code src/test/resources/features} against
 * a real Apache Iggy server started by Testcontainers.
 *
 * <p>Run with: {@code mvn verify -Pfailsafe} (or via the Maven Failsafe plugin
 * configured in the POM).
 */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.example.iggy.flink.e2e.steps")
@ConfigurationParameter(
        key = PLUGIN_PROPERTY_NAME,
        value = "pretty, html:target/cucumber-reports/report.html, json:target/cucumber-reports/report.json"
)
public class CucumberE2ERunner {
    // JUnit Platform Suite — no methods needed
}
