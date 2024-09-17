package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DmsChangeDataCountsPair;

import java.util.List;
import java.util.Map;

@Singleton
public class ChangeDataCountService {

    private final DmsChangeDataCountService dmsChangeDataCountService;
    private final RawChangeDataCountService rawChangeDataCountService;

    @Inject
    public ChangeDataCountService(
            DmsChangeDataCountService dmsChangeDataCountService,
            RawChangeDataCountService rawChangeDataCountService
    ) {
        this.dmsChangeDataCountService = dmsChangeDataCountService;
        this.rawChangeDataCountService = rawChangeDataCountService;
    }

    ChangeDataTotalCounts changeDataCounts(SparkSession sparkSession, List<SourceReference> sourceReferences, String dmsTaskId) {
        DmsChangeDataCountsPair dmsChangeDataCountsPair = dmsChangeDataCountService.dmsChangeDataCounts(sourceReferences, dmsTaskId);

        Map<String, ChangeDataTableCount> rawCounts = rawChangeDataCountService.changeDataCounts(sparkSession, sourceReferences);

        return new ChangeDataTotalCounts(
                rawCounts,
                dmsChangeDataCountsPair.getDmsChangeDataCounts(),
                dmsChangeDataCountsPair.getDmsAppliedChangeDataCounts()
        );
    }




}
