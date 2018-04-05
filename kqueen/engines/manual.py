from kqueen.config.utils import kqueen_config
from .base import BaseEngine
from kqueen.kubeapi import KubernetesAPI

import logging

logger = logging.getLogger('kqueen_api')


class ManualEngine(BaseEngine):
    """
    Manual engine is used for importing existing clusters.
    """

    name = 'manual'
    verbose_name = 'Manual Engine'
    parameter_schema = {
        'provisioner': {},
        'cluster': {
            'kubeconfig': {
                'type': 'yaml_file',
                'label': 'Kubeconfig',
                'validators': {
                    'required': True
                }
            }
        }
    }

    def __init__(self, cluster, **kwargs):
        """
        Implementation of :func:`~kqueen.engines.base.BaseEngine.__init__`
        """
        self.kubeconfig = kwargs.get('kubeconfig', {})
        super(ManualEngine, self).__init__(cluster, **kwargs)

    def cluster_list(self):
        """Manual engine don't support list of clusters"""

        return []

    def cluster_get(self):
        """
        Implementation of :func:`~kqueen.engines.base.BaseEngine.cluster_get`
        """
        try:
            client = KubernetesAPI(cluster=self.cluster)
            client.get_version()
        except Exception as e:
            msg = 'Fetching data from backend for cluster {} failed with following reason:'.format(self.cluster.id)
            logger.exception(msg)
            return {'state': kqueen_config.get('CLUSTER_ERROR_STATE')}
        return {'state': kqueen_config.get('CLUSTER_OK_STATE')}

    def provision(self):
        """
        There is no provisioning because Cluster should be already provisioned manually.

        State is updated to OK during in provision method.

        Implementation of :func:`~kqueen.engines.base.BaseEngine.provision`
        """

        self.cluster.state = kqueen_config.get('CLUSTER_OK_STATE')
        self.cluster.save()

        return True, None

    def deprovision(self):
        """
        Deprovision isn't supported by manual engine, we just pass it.

        Implementation of :func:`~kqueen.engines.base.BaseEngine.deprovision`
        """

        return True, None

    def get_kubeconfig(self):
        """Get kubeconfig of the cluster

        Manual engine don't support any loading of `kubeconfig` so we only return kubeconfig of
        cluster provided during initialization.

        Implementation of :func:`~kqueen.engines.base.BaseEngine.get_kubeconfig`
        """

        return self.kubeconfig

    def get_progress(self):
        """
        Manual engine don't report any progress because cluster is already provisioned before
        cluster is imported

        Implementation of :func:`~kqueen.engines.base.BaseEngine.get_progress`
        """

        return {
            'response': 0,
            'progress': 100,
            'result': kqueen_config.get('CLUSTER_OK_STATE'),
        }

    @classmethod
    def engine_status(cls, **kwargs):
        """Manual engine is always available.

        Implementation of :func:`~kqueen.engines.base.BaseEngine.engine_status`
        """
        return kqueen_config.get('PROVISIONER_OK_STATE')
