/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.transport.searchConfiguration;

import static org.opensearch.searchrelevance.common.PluginConstants.SEARCH_CONFIGURATION_LIST;

import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.searchrelevance.dao.ExperimentDao;
import org.opensearch.searchrelevance.dao.SearchConfigurationDao;
import org.opensearch.searchrelevance.exception.SearchRelevanceException;
import org.opensearch.searchrelevance.transport.OpenSearchDocRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to delete a search configuration
 */
public class DeleteSearchConfigurationTransportAction extends HandledTransportAction<OpenSearchDocRequest, DeleteResponse> {
    private final ClusterService clusterService;
    private final SearchConfigurationDao searchConfigurationDao;
    private final ExperimentDao experimentDao;

    @Inject
    public DeleteSearchConfigurationTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        SearchConfigurationDao searchConfigurationDao,
        ExperimentDao experimentDao
    ) {
        super(DeleteSearchConfigurationAction.NAME, transportService, actionFilters, OpenSearchDocRequest::new);
        this.clusterService = clusterService;
        this.searchConfigurationDao = searchConfigurationDao;
        this.experimentDao = experimentDao;
    }

    @Override
    protected void doExecute(Task task, OpenSearchDocRequest request, ActionListener<DeleteResponse> listener) {
        try {
            String searchConfigurationId = request.getId();
            if (searchConfigurationId == null || searchConfigurationId.trim().isEmpty()) {
                listener.onFailure(new SearchRelevanceException("searchConfigurationId cannot be null or empty", RestStatus.BAD_REQUEST));
                return;
            }
            SearchResponse experiments = experimentDao.getExperimentByFieldId(searchConfigurationId, SEARCH_CONFIGURATION_LIST);
            if (experiments != null && experiments.getHits().getTotalHits().value() > 0) {
                listener.onFailure(
                    new SearchRelevanceException(
                        "search configuration cannot be deleted as it is currently used by a experiment",
                        RestStatus.CONFLICT
                    )
                );
                return;
            }
            searchConfigurationDao.deleteSearchConfiguration(searchConfigurationId, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
