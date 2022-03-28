from __future__ import absolute_import

import logging
import os
import json
import glob
import time
import docker
import uuid
import math
import pprint

from django.db import models
from django.db.models import Subquery, OuterRef

from django.utils import timezone
from django.conf import settings
from django.utils.functional import cached_property
from django.core.validators import MaxValueValidator, MinValueValidator
from k8s.client import ClientError
from model_utils.managers import InheritanceManager
from pathlib import Path

import k8s.models.pod as pod
from k8s.models.pod import LocalObjectReference, SecretEnvSource
from k8s.models.common import ObjectMeta
from accutuning_common.models import AccutuningSiteConfiguration
from accutuning.models import (
    AccutuningEnvironment,
    VisualizedObject,
)
from accutuning.helpers import (
    ExportableModelMixin,
    ModelWorkspaceMixin,
)
from accutuning.helpers.k8s import (
    Pod as PodExt,
    PodSpecExt,
    VolumeExt,
    Toleration,
    CephFSVolumnSource,
    # PersistentVolumeClaim,
    # get_active_container_count,
)
from accutuning.constants import LONG_TASK_STATUS

from .. import AccutuningModelerException
from .evaluation import MlModelEvaluationMixin, MlEnsembleEvaluationMixin
from django.contrib.auth.models import User
# from django.db.models import Q
from accutuning.helpers.lic import check_license
from accutuning.constants import (
    EXPERIMENT_PROCESS_TYPE,
    DATASET_PROCESSING_STATUS,
    EXPERIMENT_STATUS,
    ESTIMATOR_TYPE,
    SOURCE_URI_TYPE,
    RESAMPLING_STRATEGY_TYPE,
)
from accutuning.exceptions import AvailableContainerNotExistError, ExperimentStatusError, TargetColumnError, ColumnValueError, EstimatorNotSupportError, DataSplitError, PodCreationError

logger = logging.getLogger(__name__)


DEFAULT_CLASSIFIERS = settings.DEFAULT_CLASSIFIERS
DEFAULT_REGRESSORS = settings.DEFAULT_REGRESSORS
LIGHT_CLASSIFIERS = settings.LIGHT_CLASSIFIERS
LIGHT_REGRESSORS = settings.LIGHT_REGRESSORS
AVAILABLE_CLASSIFIERS = settings.AVAILABLE_CLASSIFIERS
AVAILABLE_REGRESSORS = settings.AVAILABLE_REGRESSORS
AVAILABLE_CLUSTERS = settings.DEFAULT_CLUSTER_ALGORITHMS
DEFAULT_CLUSTER_ALGORITHMS = settings.DEFAULT_CLUSTER_ALGORITHMS
DEFAULT_CLUSTER_DIM_REDUCTION_ALGS = settings.DEFAULT_CLUSTER_DIM_REDUCTION_ALGS
DEFAULT_CLUSTER_ALGORITHMS_JSON = json.dumps(DEFAULT_CLUSTER_ALGORITHMS)
DEFAULT_CLUSTER_DIM_REDUCTION_ALGS_JSON = json.dumps(['PCA'])
DEFAULT_CLASSIFIERS_JSON = json.dumps(DEFAULT_CLASSIFIERS)
DEFAULT_REGRESSORS_JSON = json.dumps(DEFAULT_REGRESSORS)
LIGHT_CLASSIFIERS_JSON = json.dumps(LIGHT_CLASSIFIERS)
LIGHT_REGRESSORS_JSON = json.dumps(LIGHT_REGRESSORS)

AVAILABLE_SCALING_METHODS = [
    # 'none',
    'standardize',
    'minmax',
    'normalize',
]

DEFAULT_SCALING_METHODS_JSON = json.dumps([
    'none',
    'standardize',
])

AVAILABLE_FEATURES_ENGINEERINGS = [
    # 'None',
    'SelectPercentile',
    'SelectKBest',
    'SelectFwe',
    'Boruta',
    'FastICA',
    'FeatureAgglomeration',
    'Polynomial',
    'PCA',
    'Nystroem',
]

DEFAULT_FEATURES_ENGINEERINGS_JSON = json.dumps([
    'None',
    'SelectKBest',
    'SelectFwe',
])

DEFAULT_CLUSTERS_JSON = json.dumps([
    'DBINDEX',
])


class AccutuningUserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    need_to_change_password = models.BooleanField(default=False)


def _get_default_namespace():
    return AccutuningSiteConfiguration.get('ACCUTUNING_K8S_WORKER_NAMESPACE', 'default')


class ExperimentContainer(models.Model):
    experiment = models.ForeignKey('ExperimentBase', related_name='containers', on_delete=models.CASCADE)
    experiment_process_type = models.CharField(max_length=255, help_text='실험작업 타입(parse, prerprocess, cluster, dataset_eda, optuna, ensemble, modelstat, deploy, predicr, labeling 중 택1')
    uuid = models.CharField(max_length=255)
    container_id = models.CharField(max_length=64, null=True, help_text='컨테이너 고유ID')
    pod_name = models.CharField(max_length=512, null=True, help_text='파드 이름')
    created_at = models.DateTimeField(auto_now_add=True, help_text='생성일시')
    exited = models.BooleanField(default=False, help_text='종료여부')
    memory_limit = models.IntegerField(null=True, blank=True, help_text='메모리 한도(단위: Mi)')
    cpu_limit = models.IntegerField(null=True, blank=True, help_text='cpu 한도(단위: m)')
    gpu_limit = models.IntegerField(null=True, blank=True, help_text='gpu 한도(단위: m)')
    image_name = models.CharField(max_length=255, help_text='이미지 이름')
    # node_selector = models.CharField(null=True, blank=True, default=None, max_length=255, help_text='node selector 이름')
    namespace = models.CharField(null=True, blank=True, default=_get_default_namespace, max_length=1024, help_text='k8s namespace')

    workspace_path = models.CharField(
        max_length=1024, null=True, blank=True,
        help_text='docker container내부에서 기본 작업디렉토리 지정')
    volume_mount_map = models.TextField(
        null=True, blank=True,
        help_text='docker container volume map, source:target형태로 저장')
    workspace_volume_sub_path = models.CharField(
        max_length=1024, null=True, blank=True,
        help_text='기본 volume mount value값 설정에 사용합니다. host source의 docker 외부 변수 이하 하위 경로 지정')

    @property
    def status(self):

        # TODO: k8s, docker간에 status를 맞추기
        if settings.ACCUTUNING_K8S_USE:
            # if self.exited:
            #     return 'exited'
            try:
                pod = PodExt.get(self.pod_name, namespace=self.namespace)
                # https://kubernetes.io/ko/docs/concepts/workloads/pods/pod-lifecycle/
                # experiment가 자주 죽어서 임시로 logging을 상세하게 남기겠습니다.
                self.experiment.logger.info('pod-status -> {}'.format(pprint.pformat(pod.status.as_dict())))
                return pod.status.phase
            except Exception:
                self.experiment.logger.info('getting pod is failed', exc_info=True)
                # 이미 삭제처리되었을 가능성;
                # 아직 생성전인데 exited로 걸리면 celery에서 종료처리 해버리기 때문;
                # self.exited = True
                # self.save(update_fields=['exited'])
                return 'not-exist'
        else:
            if self.exited:
                return 'exited'

            client = docker.from_env()
            try:
                container = client.containers.get(self.container_id)
            except docker.errors.NotFound:
                self.exited = True
                self.save(update_fields=['exited'])
                return 'exited'
            else:
                # https://docs.docker.com/engine/api/v1.24/
                # status=(created	restarting	running	paused	exited	dead)
                if container.status == 'exited':
                    self.exited = True
                    self.save(update_fields=['exited'])
                    return 'exited'
                else:
                    return container.status

    @property
    def live(self):
        status = self.status
        if settings.ACCUTUNING_K8S_USE:
            return status in ('Pending', 'Running')
        else:
            return status in ('created', 'restarting', 'running')

    def stop(self):
        if settings.ACCUTUNING_K8S_USE:
            if settings.ACCUTUNING_WEB_TEST:
                print('-' * 100)
                print('-' * 100)
                print('-' * 100)
                print('ACCUTUNING_WEB_TEST ' * 20)
                print('-' * 100)
                print('-' * 100)
                print('-' * 100)
                return

            if AccutuningSiteConfiguration.get('ACCUTUNING_K8S_KEEP_COMPLETED_POD'):
                self.experiment.logger.info("won't be deleting k8s container because of the setting (status:{})".format(self.status))
                return

            # 21.02.25
            # 상태 기준으로 수행중(self.status == 'Running')일 경우만 삭제처리를 했는데 이렇게 하니까
            # (참고로 docker는 stop interface가 있는데 pod는 중지를 위해 삭제만 가능)
            # 어떤 pod는 남아있고 어떤 pod는 삭제되어 있고 일관성이 없어서 일괄 삭제하는 부분으로 처리변경
            # 더군다나 accuinsight+에서는 completed를 표시해주지 못해서 자원을 소모하고 있는 것으로 나온다.
            # status = self.status
            try:
                self.experiment.logger.info("stopping(deleting) k8s container (status:{})".format(self.status))            
                ret = PodExt.delete(
                    self.pod_name,
                    namespace=self.namespace,
                    body={
                        'kind': 'DeleteOptions',
                        'apiVersion': 'v1',
                        'propagationPolicy': 'Foreground'
                    })
                self.experiment.logger.info("stopping k8s container -> done - {}".format(ret))
            except Exception as e:
                self.experiment.logger.info("can't delete because status is {}".format(e))
        else:
            self.experiment.logger.info("stopping docker container (container_id:{})".format(self.container_id))
            client = docker.from_env()
            container = client.containers.get(self.container_id)
            container.stop()

    def start(self):
        if settings.ACCUTUNING_K8S_USE:
            _start = self.start_k8s
        else:
            _start = self.start_docker

        try:
            _start()
        except Exception as e:
            raise PodCreationError(str(e))

    def start_docker(self):
        import docker
        client = docker.from_env()
        volumes = {}
        if self.volume_mount_map:
            source, target = self.volume_mount_map.split(':')
            volumes.update({
                source: dict(bind=target, mode='rw')
            })
        for vol in settings.ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS:
            source, target = vol.split(':')
            volumes.update({
                source: dict(bind=target, mode='rw')
            })

        kwargs = dict(
            image=self.image_name,
            name=self.pod_name,
            environment={
                'ACCUTUNING_WORKSPACE': self.workspace_path,
                'ACCUTUNING_LOG_LEVEL': self.experiment.log_level,
                'ACCUTUNING_SENTRY_DSN': AccutuningSiteConfiguration.get('ACCUTUNING_SENTRY_DSN_FOR_MODELER'),
            },
            volumes=volumes,
            detach=True,
        )

        if settings.ACCUTUNING_WEB_TEST:
            print('-' * 100)
            print('-' * 100)
            print('-' * 100)
            print('ACCUTUNING_WEB_TEST ' * 20)
            print(kwargs)
            print('-' * 100)
            print('-' * 100)
            print('-' * 100)
            # local 환경이라고 가정하고 docker command 실행
            from pprint import pprint
            pprint(kwargs)
        else:
            container = client.containers.run(**kwargs)
            self.container_id = container.id
            self.save(update_fields=['container_id'])

    def start_k8s(self):
        ACCUTUNING_WORKER_ENV = [
            pod.EnvVar(name='ACCUTUNING_INIT', value='0'),
            pod.EnvVar(name='ACCUTUNING_WORKSPACE', value=self.workspace_path),
            pod.EnvVar(name='ACCUTUNING_EXPERIMENT', value=str(self.experiment.id)),
            pod.EnvVar(name='ACCUTUNING_EXPERIMENT_PROCESS_TYPE', value=self.experiment_process_type),
            pod.EnvVar(name='ACCUTUNING_EXPERIMENT_CONTAINER_UUID', value=self.uuid),
            pod.EnvVar(name='ACCUTUNING_LOG_LEVEL', value=self.experiment.log_level)
        ]

        if AccutuningSiteConfiguration.get('ACCUTUNING_SENTRY_DSN_FOR_MODELER'):
            ACCUTUNING_WORKER_ENV.append(
                pod.EnvVar(name='ACCUTUNING_SENTRY_DSN', value=AccutuningSiteConfiguration.get('ACCUTUNING_SENTRY_DSN_FOR_MODELER'))
            )

        try:
            # database 설정정보가 있다면 추가합니다.
            ACCUTUNING_WORKER_ENV += [
                pod.EnvVar(name='ACCUTUNING_DB_HOST', value=settings.DATABASE_HOST),
                pod.EnvVar(name='ACCUTUNING_DB_PORT', value=settings.DATABASE_PORT),
                pod.EnvVar(name='ACCUTUNING_DB_NAME', value=settings.DATABASE_NAME),
                pod.EnvVar(name='ACCUTUNING_DB_USER', value=settings.DATABASE_USER),
                pod.EnvVar(name='ACCUTUNING_DB_PASSWORD', value=settings.DATABASE_PASSWORD),
            ]
        except AttributeError:
            pass

        # k8s환경변수는 모두 전달합니다. (pod에서 api호출위한)
        ACCUTUNING_WORKER_ENV += [
            pod.EnvVar(name=k, value=str(getattr(settings, k)))
            for k in dir(settings)
            if k.startswith('ACCUTUNING_K8S_') and getattr(settings, k) is not None
        ]

        resource_limits = {}
        node_selector = settings.ACCUTUNING_K8S_NODE_TYPE_SELECTOR
        if self.memory_limit and self.memory_limit > 0:
            resource_limits['memory'] = f'{self.memory_limit}Mi'
        if self.cpu_limit and self.cpu_limit > 0:
            resource_limits['cpu'] = f'{self.cpu_limit}m'
            node_selector.update(settings.ACCUTUNING_K8S_SERVICE_TYPE_SELECTOR_FOR_CPU)
        if self.gpu_limit and self.gpu_limit > 0:
            resource_limits['nvidia.com/gpu'] = self.gpu_limit
            node_selector.update(settings.ACCUTUNING_K8S_SERVICE_TYPE_SELECTOR_FOR_GPU)

        node_type = settings.ACCUTUNING_K8S_NODETYPE
        if node_type:
            node_selector.update({'nodetype': node_type})

        self.experiment.logger.info(f'node_selector: {node_selector}')

        container_kwargs = {}
        if resource_limits:
            container_kwargs['resources'] = pod.ResourceRequirements(limits=resource_limits)

        self.experiment.logger.debug(f'container_kwargs: {container_kwargs}')

        ####################################################################################
        if settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH:
            source_mount_path = settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH
            source_mount_server = settings.ACCUTUNING_K8S_VOLUME_MOUNT_SERVER
            source_mount_type = settings.ACCUTUNING_K8S_VOLUME_MOUNT_TYPE
            if self.workspace_volume_sub_path:
                source_mount_path = os.path.join(
                    source_mount_path,
                    self.workspace_volume_sub_path,
                )

            # 항상 experiment기준으로 사용할 수 있도록 experiment의 최상위를 target으로
            # target_mount_path = self.experiment.workspace_path
            target_mount_path = settings.ACCUTUNING_WORKSPACE
            self.experiment.logger.info(
                '{}->{}'.format(source_mount_path, target_mount_path)
            )

            if source_mount_server:
                # TODO cephfs와 nfs 분기 처리 필요
                # ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS 같은 서버에 여러개의 mount를 붙일 수 없음
                # AWS (EFS) 기준

                self.experiment.logger.info(f'source_mount_type:{source_mount_type}')
                self.experiment.logger.info(f'source_mount_server:{source_mount_server}')
                if source_mount_type == 'cephfs':
                    volumes = [
                        VolumeExt(
                            name='accutuning-workspace',
                            cephfs=CephFSVolumnSource(
                                monitors=source_mount_server.split(','),
                                path=settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH,
                                secretRef=SecretEnvSource(name='ceph-admin-secret-t1'),
                                user='admin',
                            )
                        )
                    ]
                else:
                    volumes = [
                        VolumeExt(
                            name='accutuning-workspace',
                            nfs=pod.NFSVolumeSource(
                                path=settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH,
                                server=source_mount_server,
                                readOnly=False,
                            )
                        )
                    ]
                volume_mounts = [
                    pod.VolumeMount(
                        name='accutuning-workspace',
                        readOnly=False,
                        mountPath=settings.ACCUTUNING_WORKSPACE,
                    )
                ]
            else:
                volumes = [
                    VolumeExt(
                        name='accutuning-workspace',
                        hostPath=pod.HostPathVolumeSource(
                            path=source_mount_path
                        ),
                    )
                ]
                for idx, d in enumerate(settings.ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS):
                    try:
                        source, target = d.split(':')
                    except ValueError:
                        continue

                    volumes.append(
                        VolumeExt(
                            name=f'accutuning-workspace-{idx}',
                            hostPath=pod.HostPathVolumeSource(
                                path=source,
                            )
                        )
                    )

                volume_mounts = [
                    pod.VolumeMount(
                        name='accutuning-workspace',
                        readOnly=False,
                        # mountPath=settings.ACCUTUNING_WORKSPACE_PATH_FOR_CONTAINER,
                        mountPath=target_mount_path,
                    )
                ]
                for idx, d in enumerate(settings.ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS):
                    try:
                        source, target = d.split(':')
                    except ValueError:
                        continue

                    volume_mounts.append(
                        pod.VolumeMount(
                            name=f'accutuning-workspace-{idx}',
                            readOnly=False,
                            mountPath=target,
                        )
                    )
        else:
            volumes = []
            volume_mounts = []

        objectmeta = ObjectMeta(
            name=self.pod_name,
            namespace=self.namespace or AccutuningSiteConfiguration.get('ACCUTUNING_K8S_WORKER_NAMESPACE'),
            labels={
                'app': AccutuningSiteConfiguration.get('ACCUTUNING_K8S_WORKER_LABEL'),
                'type': 'automl',
                'tier': 'accutuning-worker',
            }
        )
        pod_spec = PodSpecExt(
            containers=[
                pod.Container(
                    name='accutuning',
                    image=self.image_name,
                    volumeMounts=volume_mounts,
                    imagePullPolicy=AccutuningSiteConfiguration.get('ACCUTUNING_K8S_IMAGE_PULL_POLICY'),
                    env=ACCUTUNING_WORKER_ENV,
                    **container_kwargs
                )
            ],
            restartPolicy='Never',
            # backoffLimit=0,
            imagePullSecrets=[LocalObjectReference(name=AccutuningSiteConfiguration.get('ACCUTUNING_K8S_IMAGE_PULL_SECRET'))],
            volumes=volumes,
            # tolerations=[
            #     Toleration(effect='NoSchedule', operator='Exists', key='DLWORK'),
            #     Toleration(effect='NoSchedule', operator='Exists', key='DHPWORK'),
            # ],
            nodeSelector=node_selector
        )
        pod_instance = PodExt(
            metadata=objectmeta,
            spec=pod_spec
        )
        if settings.ACCUTUNING_WEB_TEST:
            print('-' * 100)
            print('-' * 100)
            print('-' * 100)
            print('ACCUTUNING_WEB_TEST ' * 20)
            print(pod_instance)
            print('-' * 100)
            print('-' * 100)
            print('-' * 100)
            # local 환경이라고 가정하고 docker command 실행
            from pprint import pprint
            pprint(pod_instance)

        else:
            for _ in range(20):  # 먼저 뜬 pod가 종료 시 자원 회수가 안되는 경우가 있음 - 최대 1분간 재시도(3초 * 20번)
                try:
                    pod_instance.save()
                except ClientError as ce:
                    self.experiment.logger.info('Client Error 발생')
                    if 'exceeded quota' in str(ce):
                        self.experiment.logger.info('exceeded quota - sleep 3 seconds and retry')
                        time.sleep(3)
                        continue
                    else:
                        raise ce
                break
            else:
                # last try (위 loop에서 break가 아닐 경우)
                try:
                    pod_instance.save()
                except Exception as e:
                    raise PodCreationError(e)


class K8sContainerMixin(object):

    def _stop_pod(self):
        container = self.experiment.containers.get(uuid=self.container_uuid)
        # if container.live:
        container.stop()

    def _status_pod(self):
        container = self.experiment.containers.get(uuid=self.container_uuid)
        return container.status

    @property
    def _live_pod(self):
        container = self.experiment.containers.get(uuid=self.container_uuid)
        return container.live

    def _start_pod(self):
        experiment_container = self._save_pod()
        experiment_container.start()
        self.experiment.logger.info('{}-{} container started.'.format(self.experiment_process_type, self.container_uuid))

    def _save_pod(self):
        pod_name = 'accutuning-worker-{}-{}'.format(
            self.experiment_process_type.replace('_', '-'),
            self.container_uuid,
        )
        image_name = AccutuningSiteConfiguration.proc(
            self.experiment_process_type, 'image_name')
        cpu_limit = AccutuningSiteConfiguration.proc(
            self.experiment_process_type, 'cpu_default')
        memory_limit = AccutuningSiteConfiguration.proc(
            self.experiment_process_type, 'memory_default')
        gpu_limit = AccutuningSiteConfiguration.proc(
            self.experiment_process_type, 'gpu_default')

        ExperimentContainer.objects.filter(uuid=self.container_uuid).delete()
        experiment_container = ExperimentContainer(
            experiment=self.experiment,
            experiment_process_type=self.experiment_process_type,
            uuid=self.container_uuid,
            pod_name=pod_name,
            memory_limit=memory_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            image_name=image_name,
            workspace_path=self.default_docker_workspace_path,
            volume_mount_map=self.default_docker_volume_mount_map,
            # workspace_volume_sub_path=self.experiment.workspace_name,
        )
        experiment_container.save()
        self.experiment.logger.info('{}-{} container saved.'.format(self.experiment_process_type, self.container_uuid))

        return experiment_container


class MultiprocessMixin(object):
    def _start_process(self, experiment_process_type, experiment_uuid):
        from ..tasks import ml_start
        ml_start.delay(self.experiment.id, experiment_process_type, experiment_uuid)


class EventMixin(models.Model):
    log_level = models.CharField(
        default='INFO',
        max_length=10,
        choices=[
            ('CRITICAL', 'CRITICAL'),
            ('ERROR', 'ERROR'),
            ('WARNING', 'WARNING'),
            ('INFO', 'INFO'),
            ('DEBUG', 'DEBUG'),
            ('NOTSET', 'NOTSET'),
        ],
        help_text='event기록을 남기는 로그레벨을 설정합니다. 성능에 영향을 줄 수 있습니다.')

    class Meta:
        abstract = True

    @property
    def logger(self):
        # if hasattr(self, '_dblogger'):
        #     return self._dblogger
        if self.pk:
            return self._dedicated_logger
        else:
            return logging.getLogger('accutuning')

    @cached_property
    def _dedicated_logger(self):
        logger = logging.getLogger('experiment_{}'.format(self.pk))
        registered = False
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                registered = True
                break

        if not registered:

            # formatter
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s - %(message)s')

            # streamhandler
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            ch.setLevel(self.log_level)
            logger.addHandler(ch)

            # filehandler
            os.makedirs(
                os.path.join(
                    self.workspace_path,
                    'logs',
                ),
                exist_ok=True,
            )
            ch = logging.FileHandler(
                os.path.join(
                    self.workspace_path,
                    'logs',
                    'experiment.log'
                ),
                mode='w'
            )
            ch.setFormatter(formatter)
            ch.setLevel(self.log_level)
            logger.addHandler(ch)

        return logger


class ExperimentDatasetMixin(models.Model):
    split_testdata_rate = models.FloatField(
        null=True,
        blank=True,
        help_text='train/test split 비율을 설정합니다. 미설정시 전체 데이터로 train수행합니다.')
    # split_by_random = models.BooleanField(default=True)
    split_column_name = models.CharField(
        max_length=255, null=True, blank=True,
        help_text='train/valid/test를 나눌때 사용할 기준컬럼을 설정할 수 있습니다.')
    split_column_value_for_valid = models.FloatField(
        null=True, blank=True,
        help_text='valid값의 범위를 지정할때 시작 값')
    split_column_value_for_test = models.FloatField(
        null=True, blank=True,
        help_text='test값의 범위를 지정할때 시작 값')
    target_column_name = models.CharField(max_length=255, null=True, blank=True, help_text='예측하고자 하는 컬럼의 이름')
    accutuning_light = models.BooleanField(default=False, help_text='적은 자원으로 수행하는 Accutuning 모델/하이퍼파라미터 설정')

    # train configuration (train/validation)
    resampling_strategy = models.CharField(
        max_length=255,
        choices=RESAMPLING_STRATEGY_TYPE.choices(),
        default=RESAMPLING_STRATEGY_TYPE.holdout,
        help_text='리샘플링 방법. holdout, cv 중 택1'
    )
    resampling_strategy_holdout_train_size = models.FloatField(
        default=0.7,
        help_text='resampling방법이 holdout일때 유효한 설정값. 훈련/검증 비율.'
    )
    resampling_strategy_cv_folds = models.IntegerField(
        default=5,
        help_text='resampling방법이 cv일때 유효한 설정값. 몇개의 구간(fold)으로 나눌 것인지 설정'
    )  # 1~10
    resampling_strategy_partialcv_shuffle = models.BooleanField(default=True)

    use_sampling = models.BooleanField(
        default=False,
        help_text='automl 시작전 sampling 수행여부'
    )
    sampling_type = models.CharField(
        default='SIZE',
        choices=[
            ('RATIO', 'RATIO'),
            ('SIZE', 'SIZE'),
        ],
        max_length=10,
        help_text='automl 시작전 sampling 수행여부'
    )
    sampling_target = models.CharField(
        default='TRAIN',
        choices=[
            ('ALL', 'ALL'),
            ('TRAIN', 'TRAIN'),
        ],
        max_length=10,
        help_text='automl 시작전 sampling 대상'
    )
    sampling_ratio = models.FloatField(
        null=True,
        default=0.7,
        validators=[MinValueValidator(0.), MaxValueValidator(1.0)],
        help_text='샘플링 비율(0~1)')
    sampling_size = models.IntegerField(
        null=True,
        default=None,
        blank=True,
        validators=[
            MinValueValidator(10),
        ],
        help_text='샘플링 행의 수'
    )

    use_class_balancer = models.BooleanField(default=False, help_text='Sampling시 class balancer 사용 여부')
    sampling_epoch_count = models.IntegerField(default=5, help_text='Sampling시 epoch 횟수')

    @property
    def target_column(self):
        if self.target_column_name:
            return self.preprocessed_dataset.columns.filter(is_feature=True, name=self.target_column_name).first()
        else:
            return None

    @property
    def feature_names(self):
        columns = self.preprocessed_dataset.feature_names
        if self.target_column_name in columns:
            columns.remove(self.target_column_name)
        return columns

    class Meta:
        abstract = True


class ExperimentContainerCtlMixin(models.Model):
    worker_scale = models.IntegerField(default=1, help_text='optuna의 experiment생성 개수를 조절합니다.')
    parallel_running = models.BooleanField(
        default=False,
        help_text='optuna를 병렬로 실행 중인지 여부(실행 중일때만 True 입니다.)'
    )

    ################################################################################
    # container resource
    use_custom_container_resource = models.BooleanField(default=False, help_text='사용자가 임의로 컨테이너 자원을 조정할 수 있습니다')
    custom_container_gpu_limit = models.PositiveIntegerField(blank=True, null=True, default=0, help_text='사용자 지정 gpu 한도(단위: m)')
    custom_container_memory_limit = models.IntegerField(
        default=0,
        blank=True, null=True, help_text='사용자 지정 메모리 한도(단위: mb)'
    )
    custom_container_cpu_limit = models.IntegerField(
        default=0,
        blank=True, null=True, help_text='사용자 지정 cpu 한도(단위: m)'
    )

    class Meta:
        abstract = True


class ExperimentClusterMixin(models.Model):

    cluster_optimizer = models.CharField(max_length=10, choices=[
        ('random', 'random'),
        ('smac', 'smac'),
    ], default='random', help_text='클러스터 최적화 방법. smac이 random보다 빠름')
    include_cluster_algorithms_json = models.TextField(default=DEFAULT_CLUSTER_ALGORITHMS_JSON, help_text='클러스터링에 사용할 알고리즘 목록(json형식)' )
    include_cluster_dim_reduction_algs_json = models.TextField(default=DEFAULT_CLUSTER_DIM_REDUCTION_ALGS_JSON, help_text='클러스터링 차원축소에 사용할 알고리즘 목록(json형식)')
    num_clusters = models.IntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(2), MaxValueValidator(10)],
        help_text='클러스터 수'
    )

    @cached_property
    def available_cluster_algorithms(self):
        return DEFAULT_CLUSTER_ALGORITHMS

    @cached_property
    def available_cluster_dim_reduction_algs(self):
        return DEFAULT_CLUSTER_DIM_REDUCTION_ALGS

    @property
    def include_cluster_algorithms(self):
        try:
            ret = sorted(list(set(
                json.loads(self.include_cluster_algorithms_json)
            )))
        except (ValueError, TypeError):
            ret = []
        return ret

    @property
    def include_cluster_dim_reduction_algs(self):
        try:
            ret = sorted(list(set(
                json.loads(self.include_cluster_dim_reduction_algs_json)
            )))
        except (ValueError, TypeError):
            ret = []
        return ret

    class Meta:
        abstract = True


class ExperimentBase(ModelWorkspaceMixin, ExperimentContainerCtlMixin, EventMixin, models.Model):
    objects = InheritanceManager()
    name = models.CharField(max_length=255, null=True, help_text='experiment 이름을 지정할 수 있습니다.')
    description = models.TextField(null=True, blank=True, help_text='experiment의 상세설명')
    created_at = models.DateTimeField(auto_now_add=True, help_text='experiment의 생성일시')
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL, help_text='experiment의 생성유저')
    started_at = models.DateTimeField(null=True, blank=True, help_text='start event발생시간')
    stopped_at = models.DateTimeField(null=True, blank=True, help_text='stop event발생시간')
    hidden = models.BooleanField(default=False, help_text='Experiments화면에서 숨김여부. jupyter notebook와 같은 환경에서 생성하면서 web 목록에 보여주고 싶지 않을때 사용')
    random_state = models.IntegerField(default=42, null=True, help_text='random sampling시 seed값')

    private = models.BooleanField(
        default=True,
        help_text='experiment의 공개여부',
    )
    public = models.BooleanField(
        default=True,
        help_text='experiment를 실험목록에 노출여부.'
    )
    showcase = models.BooleanField(
        default=False,
        help_text='experiment의 showcase여부'
    )

    copied = models.BooleanField(default=False, help_text='복사된 Experiment인지의 여부')
    original_id = models.IntegerField(default=None, null=True, blank=True, help_text='복사된 Experiment의 경우 원본 Experiment')

    class Meta:
        # abstract = True
        ordering = ["-created_at"]

    def __str__(self):
        return f'{self.name}'

    @classmethod
    def _check_available_containers(cls, count):
        check_license()

        active_container_count = AccutuningEnvironment.active_container_count()
        total_container_count = AccutuningSiteConfiguration.get('ACCUTUNING_K8S_CONTAINER_MAX')
        if count + active_container_count > total_container_count:
            message = 'Not enough container space. {}>{}'.format(
                (count + active_container_count),
                total_container_count
            )
            raise AvailableContainerNotExistError(message)

    def _replace_content_in_columns(self, model_obj, col_names, old_text, new_text):
        """model_obj에 있는 attribute(col_names)에 대해서, 내용을 변경합니다. (FileField와 str에 경우 사용합니다.)"""
        from django.db.models.fields.files import FieldFile

        for col_name in col_names:
            col_obj = getattr(model_obj, col_name)
            if col_obj:
                if isinstance(col_obj, FieldFile):
                    fp = col_obj.name
                    col_obj.name = fp.replace(old_text, new_text)
                elif isinstance(col_obj, str):
                    new_col_obj = col_obj.replace(old_text, new_text)
                    setattr(model_obj, col_name, new_col_obj)
        model_obj.save(update_fields=col_names)


class Experiment(ExportableModelMixin, ExperimentClusterMixin, ExperimentDatasetMixin, ExperimentBase, models.Model):

    class Meta:
        # abstract = True
        ordering = ["-created_at"]

    @classmethod
    def create(cls, source_uri, uri_type=SOURCE_URI_TYPE.file, sampling_size=None, sampling_ratio=None, silent=False, aws_s3_bucket_name=None, automl_config_fp=None):
        from accutuning_preprocessor.models import Source
        source = Source.from_uri(source_uri, uri_type, aws_s3_bucket_name, automl_config_fp)
        return source.experiment(silent=silent)

    @classmethod
    def create_from_other(cls, from_id, source_uri=None, uri_type=SOURCE_URI_TYPE.file, aws_s3_bucket_name=None):
        other = cls.objects.get(pk=from_id)
        automl_config_fp = Path(other.workspace_path, other.RUN_CONFIG_FILENAME)
        if not automl_config_fp.is_file():
            other._save_config(config_fp=other.RUN_CONFIG_FILENAME)

        from accutuning_preprocessor.models import Source
        if source_uri:
            source = Source.from_uri(source_uri, uri_type, aws_s3_bucket_name, automl_config_fp)
        else:
            other_source = Source.objects.filter(dataset__id=other.dataset.id).first()
            source = Source.from_uri(other_source.file.name, "FILE", automl_config_fp)
        return source.experiment()

    @property
    def last_error(self):
        ep = ExperimentProcess.objects.filter(
            experiment=self,
        ).order_by('-pk').first()
        if ep and ep.error:
            return ep.error_message

    @property
    def last_error_code(self):
        ep = ExperimentProcess.objects.filter(
            experiment=self,
        ).order_by('-pk').first()
        if ep and ep.error and ep.error_code:
            return ep.error_code

    @property
    def log_files(self):
        return [
            os.path.join(self.workspace_name, fp[len(self.workspace_path) + 1:])
            for fp in glob.glob(os.path.join(self.workspace_path, '**', '*.log'))
        ]

    @property
    def dataset(self):
        return self.dataset_steps.order_by('id').first()

    @property
    def preprocessed_datasets(self):
        ds = self.dataset
        if ds is None:
            raise AccutuningModelerException('dataset is not ready')
        ret = [ds]
        while True:
            ppd = ds.processed_dataset
            if ppd is None:
                break
            ret.append(ppd)
            ds = ppd
        return ret

    @property
    def preprocessed_dataset(self):
        return self.preprocessed_datasets[-1]

    @property
    def preprocessing_count(self):
        return len(self.preprocessed_datasets)

    @property
    def preprocessors_info(self):
        dss = self.preprocessed_datasets
        if len(dss) <= 1:
            return []
        return [
            ds.preprocessor_info
            for ds in dss[:-1]
        ]

    # pycaret
    pycaret_conffile = models.FileField(max_length=1024, null=True, blank=True, help_text='pyacaret용 설정파일')
    pycaret_setupfile = models.FileField(max_length=1024, null=True, blank=True, help_text='pyacaret용 설치파일')

    # optimizing
    timeout = models.IntegerField(default=60 * 60, help_text='전체 학습시간(초단위)')
    max_eval_time = models.IntegerField(default=300, help_text='각 모델별 최대 학습시간(초단위)')
    estimator_type = models.CharField(
        max_length=20,
        default=ESTIMATOR_TYPE.classifier,
        choices=ESTIMATOR_TYPE.choices(),
        help_text='지도학습(분류 혹은 회귀), 비지도학습 여부를 선택합니다.'
    )

    use_earlystopping = models.BooleanField(default=True, help_text='optimizing과정에서 earlystop을 수행합니다.')
    use_optuna = models.BooleanField(default=False, help_text='optuna로 최적화를 수행합니다. (container는 1개 이상)')
    use_ensemble = models.BooleanField(default=True, help_text='생성되는 모델들을 기준으로 ensemble을 만드는 컨테이너를 수행합니다.')

    ensemble_size = models.IntegerField(default=20, help_text='ensemble build시 조합할 수 있는 최대 model의 개수')
    ensemble_n_best = models.IntegerField(
        default=20,
        help_text='ensemble build시 상위 n_best 개의 model만 후보로 사용합니다.',
    )
    metric = models.CharField(
        default='accuracy',
        max_length=255,
        choices=[
            ('accuracy', 'accuracy'),
            ('balanced_accuracy', 'balanced_accuracy'),
            ('f1', 'f1'),
            ('f1_macro', 'f1_macro'),
            ('f1_micro', 'f1_micro'),
            ('f1_samples', 'f1_samples'),
            ('f1_weighted', 'f1_weighted'),
            ('roc_auc', 'roc_auc'),
            ('roc_auc_ovr', 'roc_auc_ovr'),
            ('precision', 'precision'),
            ('precision_macro', 'precision_macro'),
            ('precision_micro', 'precision_micro'),
            ('precision_samples', 'precision_samples'),
            ('precision_weighted', 'precision_weighted'),
            ('average_precision', 'average_precision'),
            ('recall', 'recall'),
            ('recall_macro', 'recall_macro'),
            ('recall_micro', 'recall_micro'),
            ('recall_samples', 'recall_samples'),
            ('recall_weighted', 'recall_weighted'),
            ('neg_log_loss', 'neg_log_loss'),
            # ('pac_score', 'pac_score'),
            ('r2', 'r2'),
            ('neg_mean_squared_error', 'neg_mean_squared_error'),
            ('neg_mean_absolute_error', 'neg_mean_absolute_error'),
            ('neg_median_absolute_error', 'neg_median_absolute_error'),
            ('neg_root_mean_squared_error', 'neg_root_mean_squared_error'),
            # clustering
            ('silhouette_coefficient', 'silhouette_coefficient'),
            # ('dunn_index', 'dunn_index'),
        ],
        help_text='scoring함수를 지정합니다. estimator유형과 target의 바이너리유형에 따라 제약사항이 있습니다.',
    )
    include_estimators_json = models.TextField(default=DEFAULT_CLASSIFIERS_JSON, null=True, blank=True)

    use_autopipeline = models.BooleanField(
        default=True,
        help_text='최적화 과정에 pipeline build 포함여부를 결정합니다.'
    )
    include_timeseries = models.BooleanField(
        default=False,
        help_text='Timeseries data가 들어올 경우, Feature convert, lag 처리 등 방법을 사용합니다.'
    )
    include_scaling_methods_json = models.TextField(default=DEFAULT_SCALING_METHODS_JSON, null=True, blank=True)
    include_feature_engineerings_json = models.TextField(default=DEFAULT_FEATURES_ENGINEERINGS_JSON, null=True, blank=True)
    include_one_hot_encoding = models.BooleanField(
        default=True,
        help_text='OHE를 최적화과정에서 포함시킵니다. (설정여부는 optuna에서만 동작합니다. autosklearn은 무조건 포함합니다.)'
    )
    include_variance_threshold = models.BooleanField(
        default=True,
        help_text='Feature selector that removes all low-variance features. (설정여부는 optuna에서만 동작합니다.)'
    )

    INITIAL_CONFIG_FILENAME = 'initial_automl_setting.json'  # parse 후 automl 설정을 저장할 파일 경로 (설정 초기화시 사용)
    RUN_CONFIG_FILENAME = 'run_automl_setting.json'  # automl run 할 때 설정을 저장할 파일 경로

    @property
    def setup_info(self):
        if self.pycaret_setupfile:
            ppath = Path(self.pycaret_setupfile.path)
            if ppath.exists():
                d = json.loads(ppath.read_text())
                # d_new = {}
                # for key, value in d.items():
                #     if key.endswith('_fp') and value:
                #         try:
                #             d_new[key + '_url'] = info.context.build_absolute_uri('/media/' + value)
                #         except (KeyError, ValueError, TypeError):
                #             pass
                # d.update(d_new)
                return d
        return None

    def get_leaderboard_estimator_group_queryset(self):
        # rest-api, graphql에서 공통으로 사용하기 위함.
        model_qs = ModelBase.objects.filter(experiment=self, estimator_name=OuterRef('estimator_name')).order_by('-score')
        try:
            lst = list(ModelBase.objects.filter(experiment=self).order_by('estimator_name').values('estimator_name').annotate(
                last_created_at=models.Max('created_at'),
                count=models.Count('id'),
                score=models.Max('score'),
                created_at=Subquery(model_qs.values('created_at')[:1]),
                train_score=Subquery(model_qs.values('train_score')[:1]),
                valid_score=Subquery(model_qs.values('valid_score')[:1]),
                test_score=Subquery(model_qs.values('test_score')[:1]), 
                filesize=Subquery(model_qs.values('filesize')[:1]),
                id=Subquery(model_qs.values('id')[:1]),
            ).order_by('-score'))
        except TypeError:  # subquery에서 None다룰때
            lst = []
        estimators_included = json.loads(self.include_estimators_json)
        estimators_to_be_built = set(estimators_included) - set(
            e['estimator_name']
            for e in lst
        )
        for e in estimators_to_be_built:
            lst.append(dict(
                estimator_name=e,
                last_created_at=None,
                count=0,
                score=None,
                train_score=None,
                valid_score=None,
                test_score=None,
                filesize=None,
                id=None,
            ))
        processes = self.processes.filter(
            finished_at__isnull=True,
            experiment_process_type=EXPERIMENT_PROCESS_TYPE.optuna,
        ).all()

        jobs = dict()
        for proc in processes:
            trial = proc.current_trial
            if trial is None:
                continue
            estimator = trial['params'].get('classifier') or trial['params'].get('regressor')
            elapsed_sec = trial['elapsed_sec']
            jobs[estimator] = dict(elapsed_sec=elapsed_sec, id=proc.id)
        for e in lst:
            if e['estimator_name'] in jobs:
                e['job'] = 'job#' + str(jobs[e['estimator_name']]['id'])
                e['job_elapsed_sec'] = jobs[e['estimator_name']]['elapsed_sec']
            else:
                e['job'] = None
                e['job_elapsed_sec'] = None
        return lst

    @property
    def default_estimators_json(self):
        if self.accutuning_light:
            if self.estimator_type == ESTIMATOR_TYPE.classifier:
                return LIGHT_CLASSIFIERS_JSON
            elif self.estimator_type == ESTIMATOR_TYPE.regressor:
                return LIGHT_REGRESSORS_JSON
            else:
                return DEFAULT_CLUSTERS_JSON
        else:
            if self.estimator_type == ESTIMATOR_TYPE.classifier:
                return DEFAULT_CLASSIFIERS_JSON
            elif self.estimator_type == ESTIMATOR_TYPE.regressor:
                return DEFAULT_REGRESSORS_JSON
            else:
                return DEFAULT_CLUSTERS_JSON

    @cached_property
    def include_feature_engineerings(self):
        try:
            ret = sorted(list(set(
                json.loads(self.include_feature_engineerings_json)
            )))
        except (ValueError, TypeError):
            ret = []

        if ret:
            ret.append('None')
        return ret

    @cached_property
    def include_scaling_methods(self):
        try:
            ret = sorted(list(set(
                json.loads(self.include_scaling_methods_json)
            )))
        except (ValueError, TypeError):
            ret = []

        if ret:
            ret.append('none')
        return ret

    @cached_property
    def include_estimators(self):
        try:
            return sorted(list(set(json.loads(self.include_estimators_json))))
        except (ValueError, TypeError):
            return []

    @property
    def best_score(self):
        lst = self.modelbase_set.exclude(score=None).exclude(dummy=True).order_by(
            '-score' if self.greater_is_better else 'score'
        ).first()
        if lst is not None:
            try:
                return math.floor(lst.score * 100) / 100
            except Exception:
                self.logger.info('failed to getting the best score', exc_info=True)
                return None
        else:
            return None

    @property
    def models_cnt(self):
        return self.modelbase_set.count()

    @property
    def deployments_cnt(self):
        # return self.modelbase_set.count()
        from accutuning_validator.models import Deployment
        return Deployment.objects.filter(experiment=self).count()

    @property
    def ml_models(self):
        return MlModel.objects.filter(experiment=self)

    @property
    def ml_ensembles(self):
        return MlEnsemble.objects.filter(experiment=self)

    @property
    def status_created(self):
        return self.processes.filter(
            experiment_process_type=EXPERIMENT_PROCESS_TYPE.parse,
            finished_at__isnull=False
        ).count() > 0

    @property
    def status_learning(self):
        return self.processes.filter(
            experiment_process_type__in=[
                EXPERIMENT_PROCESS_TYPE.cluster,
                EXPERIMENT_PROCESS_TYPE.optuna,
                EXPERIMENT_PROCESS_TYPE.ensemble,
                EXPERIMENT_PROCESS_TYPE.preprocess,
            ],
            finished_at__isnull=True).count() > 0

    @property
    def status_error(self):
        return self.processes.filter(
            experiment_process_type__in=[
                EXPERIMENT_PROCESS_TYPE.parse,
                EXPERIMENT_PROCESS_TYPE.preprocess,
                EXPERIMENT_PROCESS_TYPE.optuna,
                EXPERIMENT_PROCESS_TYPE.ensemble,
            ],
            error=True).count() > 0 and self.models_cnt == 0

    @property
    def status(self):
        if self.status_error:
            return EXPERIMENT_STATUS.error
        if self.stopped_at is not None:
            if self.processes.filter(finished_at__isnull=True).count() > 0:
                return EXPERIMENT_STATUS.finishing
            return EXPERIMENT_STATUS.finished
        if self.started_at:
            if self.status_learning:
                return EXPERIMENT_STATUS.learning
            else:
                return EXPERIMENT_STATUS.finished
        else:
            if not self.status_created:
                return EXPERIMENT_STATUS.creating

            processes = self.processes.filter(use_container=True).values_list('experiment_process_type', 'finished_at', 'error')
            if any(1 for p in processes if p[1] is None):
                return EXPERIMENT_STATUS.working  # preprocess, tagtext

            return EXPERIMENT_STATUS.ready

    @cached_property
    def available_feature_engineerings(self):
        return AVAILABLE_FEATURES_ENGINEERINGS

    @cached_property
    def available_scaling_methods(self):
        return AVAILABLE_SCALING_METHODS

    @cached_property
    def greater_is_better(self):
        return self.metric not in [
            'log_loss',
            # 'neg_mean_squared_error',
            # 'neg_mean_absolute_error',
            # 'neg_median_absolute_error',
        ]

    @cached_property
    def available_metrics(self):
        '''현재 설정 기준으로 사용가능한 metric 이름목록'''
        # sklearn_metrics = set(SCORERS.keys())
        if self.estimator_type == ESTIMATOR_TYPE.classifier:
            dataset = self.dataset
            if dataset:
                # target_column = dataset.columns.filter(is_target=True).first()
                target_column = self.target_column
                if target_column and target_column.task_type == 'binary':
                    return sorted([
                        'accuracy',
                        'balanced_accuracy',
                        'f1',
                        'f1_macro',
                        'f1_micro',
                        'f1_weighted',
                        'roc_auc',
                        'precision',
                        'precision_macro',
                        'precision_micro',
                        'precision_weighted',
                        'average_precision',
                        'recall',
                        'recall_macro',
                        'recall_micro',
                        'recall_weighted',
                        'neg_log_loss',
                        # 'pac_score',
                    ])
            return sorted([
                'accuracy',
                'balanced_accuracy',
                'f1_macro',
                'f1_micro',
                'f1_weighted',
                'roc_auc_ovr',
                'precision_macro',
                'precision_micro',
                'precision_weighted',
                'recall_macro',
                'recall_micro',
                'recall_weighted',
                'neg_log_loss',
            ])
        elif self.estimator_type == ESTIMATOR_TYPE.regressor:
            return sorted([
                'r2',
                'neg_mean_squared_error',
                'neg_mean_absolute_error',
                'neg_median_absolute_error',
                'neg_root_mean_squared_error',
            ])
        else:
            return sorted([
                'silhouette_coefficient',
                # 'dunn_index',
            ])

    @cached_property
    def available_estimators(self):
        if self.estimator_type == ESTIMATOR_TYPE.classifier:
            return AVAILABLE_CLASSIFIERS
        elif self.estimator_type == ESTIMATOR_TYPE.regressor:
            return AVAILABLE_REGRESSORS
        else:
            return AVAILABLE_CLUSTERS

    def validate(self, *args, **kwargs):

        if self.status not in [EXPERIMENT_STATUS.ready]:
            raise ExperimentStatusError('experiment status is not ready.')

        # if self.preprocessed_dataset and self.preprocessed_dataset.processing_status != DATASET_PROCESSING_STATUS.ready:
        #     raise AccutuningModelerException('last dataset is not ready(status:{})'.format(
        #         self.preprocessed_dataset.processing_status
        #     ))

        # if self.estimator_type == ESTIMATOR_TYPE.clustering:
        #     self._check_available_containers(1)
        #     if not self.include_cluster_algorithms:
        #         raise AccutuningModelerException('must one cluster alogorithm at least')
        # else:
        if self.target_column is None:
            raise TargetColumnError('target column must be specified correctly.')

        missing_cols = [
            column.name
            for column in self.preprocessed_dataset.columns.all()
            if column.missing > 0 and column.imputation == 'NONE' and column.is_feature
        ]

        if missing_cols:
            raise ColumnValueError('Impute strategies should be specified for columns with missing values ({})'.format(','.join(missing_cols)))

        if self.target_column.missing > 0 and self.target_column.imputation == 'NONE':
            raise TargetColumnError('The target column has null values which should be processed by imputation.')

        if self.split_column_name:
            # if not self.split_column_value_for_test:
            #     raise AccutuningModelerException(
            #         'A proper split value for test must be specified.')
            if self.resampling_strategy == RESAMPLING_STRATEGY_TYPE.holdout and not self.split_column_value_for_valid:
                raise DataSplitError(
                    'A proper split value for validation must be specified.')

        include_estimators = self.include_estimators
        if include_estimators:
            if (set(include_estimators) & set(self.available_estimators)) != set(include_estimators):
                self.logger.debug(include_estimators)
                self.logger.debug(self.available_estimators)
                raise EstimatorNotSupportError('There are estimators be not supported.')

            # estimator 갯수만큼 container를 띄울 것이라, parallel count(worker_scale)가 estimator 갯수를 넘어설 수 없다.
            if (estimator_len := len(include_estimators)) < self.worker_scale:
                self.worker_scale = estimator_len
                self.save(update_fields=['worker_scale'])  # 따로 오류를 내진 않고 고쳐줌

            if self.use_optuna:
                count = self.worker_scale
            else:
                count = 1

            self._check_available_containers(count)

    def _check_estimator_metric_pair(self):

        self.__dict__.pop('available_metrics', None)  # delete cache
        set_to_default = False
        available_metrics = self.available_metrics
        if self.metric and self.metric not in available_metrics:
            self.logger.warn(f'metric "{self.metric}" cannot be used in this experiment. so set to defaults')
            set_to_default = True

        if self.metric is None or set_to_default:
            if self.estimator_type == ESTIMATOR_TYPE.classifier:
                self.metric = 'accuracy'
            elif self.estimator_type == ESTIMATOR_TYPE.regressor:
                self.metric = 'neg_mean_squared_error'
            else:
                self.metric = available_metrics[0]

    def _check_estimators_pair(self):
        self.__dict__.pop('available_estimators', None)  # delete cache
        self.__dict__.pop('include_estimators', None)
        if self.accutuning_light:
            logger.info('accutuning light set. so set to accutuning light defaults')
            self.include_estimators_json = self.default_estimators_json
            self.__dict__.pop('include_estimators', None)  # delete cache again;
        if (set(self.include_estimators) & set(self.available_estimators)) != set(self.include_estimators):
            logger.info('invalid estimators set. so set to defaults')
            self.include_estimators_json = self.default_estimators_json
            self.__dict__.pop('include_estimators', None)  # delete cache again;

    def _check_cluster_target(self):
        # if self.estimator_type == ESTIMATOR_TYPE.clustering:
        #     self.target_column_name = None
        # elif self.target_column_name is None:
        if self.target_column_name is None:
            try:
                ds = self.preprocessed_dataset
            except AccutuningModelerException:
                return

            col = ds.columns.filter(is_feature=True).order_by('-pk').first()
            if col:
                self.target_column_name = col.name

    def _check_split_values(self):
        if self.resampling_strategy_holdout_train_size and (self.resampling_strategy_holdout_train_size <= 0 or self.resampling_strategy_holdout_train_size >= 1):
            self.resampling_strategy_holdout_train_size = None
            logger.info('split_validdata_rate value is invalid and set to None')
        if self.split_testdata_rate and (self.split_testdata_rate <= 0 or self.split_testdata_rate >= 1):
            self.split_testdata_rate = None
            logger.info('split_testdata_rate value is invalid and set to None')

    def save(self, *args, **kwargs):
        self._check_estimator_metric_pair()
        self._check_estimators_pair()
        self._check_cluster_target()
        self._check_split_values()
        self.logger.setLevel(getattr(logging, self.log_level))
        return super().save(*args, **kwargs)

    def _save_config(self, config_fp=RUN_CONFIG_FILENAME):
        """현재 설정을 json으로 저장, config_fp는 experiment 폴더에 대한 상대경로"""

        EXPERIMENT_CONFIG_ATTRS = [
            'estimator_type', 'metric', 'target_column_name',  # GENERAL
            'resampling_strategy', 'resampling_strategy_cv_folds', 'resampling_strategy_holdout_train_size', 'split_testdata_rate', 'split_column_value_for_test', # Partitioning
            'use_sampling', 'sampling_target', 'sampling_size', 'use_class_balancer',  # Sampling
            'use_ensemble', 'include_estimators_json',
            'include_one_hot_encoding', 'include_variance_threshold', 'include_scaling_methods_json', 'include_feature_engineerings_json',
            'timeout', 'max_eval_time', 'use_earlystopping', 'log_level', 'random_state',
            'worker_scale', 'use_custom_container_resource',  # Advanced Configuration
        ]
        DATASET_CONFIG_ATTRS = ['outlier_use', 'outlier_threshold', 'outlier_strategy', 'recommend_use']  # 설정 버튼 + 추천 버튼
        COLUMN_CONFIG_ATTRS = ['name', 'datatype', 'is_feature', 'imputation', 'use_outlier', 'transformation_strategy']

        ds = self.preprocessed_dataset
        from accutuning_preprocessor.models import Column
        columns = Column.objects.filter(dataset__id=ds.id)

        # Object에서 list에 있는 attribute를 찾아 dictionary로 반환한다.
        make_attr_dict = lambda obj, attr_list: {attr: getattr(obj, attr) for attr in attr_list}

        config_dict = make_attr_dict(self, EXPERIMENT_CONFIG_ATTRS)
        dataset_dict = make_attr_dict(ds, DATASET_CONFIG_ATTRS)
        column_list = [make_attr_dict(column, COLUMN_CONFIG_ATTRS) for column in columns]  # 만약 추천 기능을 사용할 경우 column 설정은 무시되고 schema만 맞는지 체크함

        dataset_dict['columns'] = column_list
        config_dict['dataset'] = dataset_dict

        dump_file = Path(self.workspace_path) / config_fp

        with open(dump_file, 'w') as f:
            json.dump(config_dict, f)

    def _load_config(self, config_fp):
        """설정을 load하여 반영함, config_fp는 ACCUTUNING_WORKSPACE에 대한 상대경로"""

        with open(Path(settings.ACCUTUNING_WORKSPACE, config_fp), 'r') as f:
            config_dict = json.load(f)

        dataset_dict = config_dict.pop('dataset')
        column_list = dataset_dict.pop('columns')

        from accutuning_preprocessor.models import Column
        ds = self.preprocessed_dataset
        columns = Column.objects.filter(dataset__id=ds.id)

        # 먼저 config의 컬럼 정보와 현재 실험의 컬럼 정보의 schema가 같은지 확인해야 함 - 컬럼 이름만으로 확인함(대신 순서와 갯수 모두 같아야 함)
        for column_dict, column in zip(column_list, columns):
            if column_dict.get('name') != getattr(column, 'name'):
                raise AccutuningModelerException('Schema does NOT match config file')

        self.logger.info('schema check complete')

        def _config_change(conf_dict, obj):
            """conf_dict에 있는 항목을 obj에 setting한다. 값이 변경되었는지 여부를 리턴한다."""
            config_changed = False
            for k, v in conf_dict.items():
                if getattr(obj, k) != v:
                    setattr(obj, k, v)
                    if not config_changed:
                        config_changed = True
            return config_changed

        if _config_change(config_dict, self):
            self.logger.info('experiment 중 변경된 부분이 있어 save 합니다.')
            self.save()

        if _config_change(dataset_dict, ds):
            self.logger.info('dataset 중 변경된 부분이 있어 save 합니다.')
            ds.save()

        if dataset_dict.get('recommend_use'):  # 추천 기능 사용시 Column 설정 정보는 무시 -> 이 데이터에 맞는 추천을 해야하기 때문
            self.logger.info('추천 기능을 사용합니다. 이 데이터에 맞는 추천 기능을 사용하느라 기존 column 설정정보는 무시됩니다.')
            ds.recommend_configuration()
        else:
            for column_dict, column in zip(column_list, columns):
                if _config_change(column_dict, column):
                    self.logger.info('column 중 변경된 부분이 있어 save 합니다.')
                    column.save()

    def _start_with_airflow(self, experiment_process_type, experiment_target=None, proceed_next=False, output_file_prefix=None, input_args=None):
        # TODO 이 많은 파라미터가 무슨 의미인지 모르겠는데 정리해야겠다.
        # output_file_prefix는 안 받아도 됨 : experimentprocess_type으로부터 모두 구해와도 됨
        # select *
        # from accutuning_modeler_experimentprocess
        # where output_file_prefix not like CONCAT(experiment_process_type, '%')

        self.logger.info('_start_with_airflow experiment({})'.format(experiment_process_type))

        def get_env_vars():
            return {k: v for k, v in os.environ.items() if k.startswith('ACCUTUNING_') or k.startswith('DJANGO')}

        def get_worker_env_vars():
            environment = {k: v for k, v in os.environ.items() if k.startswith('ACCUTUNING_K8S_')}

            if settings.ACCUTUNING_K8S_USE:  # k8s일 경우 복잡함. TODO k8s의 경우 Env var 셋팅
                environment.update({
                    # 'ACCUTUNING_WORKSPACE': workspace_path,  # pre(before) pod 안에서 구해옴
                    'ACCUTUNING_LOG_LEVEL': self.log_level,
                    'ACCUTUNING_SENTRY_DSN': AccutuningSiteConfiguration.get('ACCUTUNING_SENTRY_DSN_FOR_MODELER'),
                })
            else:  # docker의 경우는 simple
                environment.update({
                    # 'ACCUTUNING_WORKSPACE': workspace_path,  # pre(before) pod 안에서 구해옴
                    'ACCUTUNING_LOG_LEVEL': self.log_level,
                    'ACCUTUNING_SENTRY_DSN': AccutuningSiteConfiguration.get('ACCUTUNING_SENTRY_DSN_FOR_MODELER'),
                })

            return environment

        accutuning_volume_mount = [
            "/var/run/docker.sock:/var/run/docker.sock",  # TODO docker 일 경우 해당
            f"{settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH}:/workspace",  # TODO 해당 값 없을경우 처리
        ]
        worker_volume_mount = f"{settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH}:/workspace"

        call_cmd = ''
        from accutuning_common.airflow import call_dag
        if settings.ACCUTUNING_K8S_USE == 1:
            #   'ACCUTUNING_WORKSPACE':'{{dag_run.conf["ACCUTUNING_WORKSPACE"]}}',
            #   'ACCUTUNING_LOG_LEVEL':'{{dag_run.conf["ACCUTUNING_LOG_LEVEL"]}}',
            #   'ACCUTUNING_USE_LABELER':'{{dag_run.conf["ACCUTUNING_USE_LABELER"]}}',
            #   'ACCUTUNING_USE_CLUSTERING':'{{dag_run.conf["ACCUTUNING_USE_CLUSTERING"]}}',
            #   'DJANGO_SETTINGS_MODULE':'{{dag_run.conf["DJANGO_SETTINGS_MODULE"]}}'                
            # print("before ====={}".format(get_pod_command(self.id, experiment_process_type, container_uuid, 'before')))  
            # print("after ====={}".format(get_pod_command(self.id, experiment_process_type, container_uuid, 'after')))  
            call_cmd = 'ml_run_k8s'
            dag_parameter = {
                "accutuning_image": AccutuningSiteConfiguration.get('ACCUTUNING_K8S_WORKER_IMAGENAME'),
                "worker_image": AccutuningSiteConfiguration.proc(experiment_process_type, 'image_name'),
                # "timeout": timeout,  # TODO timeout에 대한 처리
                "accutuning_env_vars": json.dumps(get_env_vars()),
                "worker_env_vars": json.dumps(get_worker_env_vars()),
                "accutuning_volume_mount": ','.join(accutuning_volume_mount),  # TODO pod 안에서는 volume mount 정보를 구해올 수 있는 방법이 없구나...
                "worker_volume_mount": worker_volume_mount,  # TODO k8s일 때와, docker에서 ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS 셋팅시
                # "before_command": get_pod_command(self.id, experiment_process_type, container_uuid, 'before'),
                # "after_command": get_pod_command(self.id, experiment_process_type, container_uuid, 'after'),
                "ACCUTUNING_WORKSPACE":settings.ACCUTUNING_WORKSPACE,
                # "ACCUTUNING_WORKSPACE":settings.ACCUTUNING_WORKSPACE,
                "ACCUTUNING_LOG_LEVEL":settings.ACCUTUNING_LOG_LEVEL,
                "ACCUTUNING_USE_LABELER":settings.ACCUTUNING_USE_LABELER,
                "ACCUTUNING_USE_CLUSTERING":settings.ACCUTUNING_USE_CLUSTERING,
                "DJANGO_SETTINGS_MODULE":settings.DJANGO_SETTINGS_MODULE,
                "ACCUTUNING_APP_IMAGE":settings.ACCUTUNING_APP_IMAGE,
                "ACCUTUNING_WORKER_IMAGE":settings.ACCUTUNING_WORKER_IMAGE,
                "ACCUTUNING_LOG_LEVEL":settings.ACCUTUNING_LOG_LEVEL,
                "DJANGO_SETTINGS_MODULE":settings.DJANGO_SETTINGS_MODULE,
                # "ACCUTUNING_DJANGO_COMMAND":get_command_name(experiment_process_type),
                "ACCUTUNING_EXPERIMENT_ID":self.id,
                # "ACCUTUNING_UUID":container_uuid,
                "ACCUTUNING_TIMEOUT":100,         
                # "ACCUTUNING_WORKER_WORKSPACE":exp_container.workspace_path,     
                "experiment_id": self.id,
                "experiment_process_type": experiment_process_type,
                "experiment_target": experiment_target,
                "proceed_next": proceed_next,   
                "use_ensemble": self.use_ensemble, 
            }
        else:
            call_cmd = 'ml_run_docker'
            dag_parameter = {
                "accutuning_image": AccutuningSiteConfiguration.get('ACCUTUNING_K8S_WORKER_IMAGENAME'),
                "worker_image": AccutuningSiteConfiguration.proc(experiment_process_type, 'image_name'),
                # "timeout": timeout,  # TODO timeout에 대한 처리
                "accutuning_env_vars": json.dumps(get_env_vars()),
                "worker_env_vars": json.dumps(get_worker_env_vars()),
                "accutuning_volume_mount": ','.join(accutuning_volume_mount),  # TODO pod 안에서는 volume mount 정보를 구해올 수 있는 방법이 없구나...
                "worker_volume_mount": worker_volume_mount,  # TODO k8s일 때와, docker에서 ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS 셋팅시
                "experiment_id": self.id,
                "experiment_process_type": experiment_process_type,
                "experiment_target": experiment_target,
                "proceed_next": proceed_next,
            }
        call_dag(call_cmd, dag_parameter)  # TODO 호출이 제대로 되었는지 확인

    def _start(self, experiment_process_type, experiment_target=None, proceed_next=False, output_file_prefix=None, input_args=None):
        self.logger.info('_start experiment({})'.format(experiment_process_type))
        self.logger.info('accutuning_modeler models.py experiment _start')

        if not self.parallel_running:  # parallel_running의 경우 이미 앞에서 체크&확보되어 있음(start()-validate() 메소드)
            self._check_available_containers(1)

        # optuna에서 multi-process로 돌릴 때 사용할 변수들
        concurrency = 1
        chunks = [None]

        if experiment_process_type == EXPERIMENT_PROCESS_TYPE.optuna:
            concurrency = self.worker_scale
            if concurrency > 1:
                chunks = [[estimator] for estimator in self.include_estimators]

            if self.started_at is None:
                '''
                실험이 시작되었다는 것을 관리하기 위함
                pp의 경우 예전에는 단독으로 처리가 가능해서 optuna시작할때야 비로소
                automl시작되었음을 알 수 있어서 이렇게 관리했었음;
                로직 개선 일부 필요
                '''
                self.started_at = timezone.now()
                self.stopped_at = None
                self.save(update_fields=[
                    'started_at',
                    'stopped_at',
                ])

        if experiment_process_type in [
            EXPERIMENT_PROCESS_TYPE.optuna,
            EXPERIMENT_PROCESS_TYPE.ensemble
        ] and self.timeout > 0:
            # should_finish_at = timezone.now() + timezone.timedelta(seconds=self.timeout)
            timeout = self.timeout
        else:
            # should_finish_at = None
            timeout = 0

        uuids = [str(uuid.uuid4()).replace('-', '') for _ in chunks]

        for idx, (estimators_chunk, experiment_uuid) in enumerate(zip(chunks, uuids)):
            exp_proc = ExperimentProcess(
                experiment=self,
                experiment_process_type=experiment_process_type,
                experiment_target=experiment_target,
                container_uuid=experiment_uuid,
                proceed_next=proceed_next,
                # should_finish_at=should_finish_at,
                estimators_chunk_json=json.dumps(estimators_chunk),
                input_args_json=json.dumps(input_args),
                output_file_prefix=output_file_prefix or f'{experiment_process_type}{idx}',
            )
            exp_proc.save()
            self.logger.info(f'experiment process idx:{idx} saved')

        # concurrency 관계없이, process_type도 관계없이 다 ml_start_with_concurrency를 거쳐 ml_start로 넘어간다.
        from ..tasks import ml_start_with_concurrency
        ml_start_with_concurrency.delay(self.id, experiment_process_type, uuids, concurrency, timeout)

    def start(self, automl_config_fp=None):
        """run automl을 수행합니다.

        Remark:
            이 함수에서는 run automl 이전 validation 및 setting 과정을 진행하고, 마지막 preprocess 작업을 호출합니다.
            실제 automl 수행은 proprocess 수행 마지막에 _start()를 호출함으로 수행됩니다.
            See also ml_preprocess.py -> process_post() method)
        """

        if automl_config_fp:
            self._load_config(automl_config_fp)
            self.logger.info('load config complete!!')

        # 기본작업환경검사
        self.validate()  # 컨테이너 갯수 체크도 여기 안에서 함

        self._save_config(config_fp=self.RUN_CONFIG_FILENAME)  # automl 설정을 json으로 저장

        # 미리 해당 갯수만큼의 가용 컨테이너를 확보하고자 함(실제 automl 수행시 컨테이너 부족으로 오류나는 것 방지)
        if self.worker_scale > 1:
            self.parallel_running = True  # ml_start_with_concurrency()의 마지막 부분에서 다시 False로 바꿈
            self.save(update_fields=['parallel_running'])

        # automl 수행 전 마지막 preprocess
        self.preprocessed_dataset.preprocess_using_container(proceed_next=True)  # 이 컨테이너가 끝나는 시점에 _start()를 호출함

    def stop(self):
        if self.stopped_at is None:
            self.stopped_at = timezone.now()
            self.save(update_fields=['stopped_at'])

        self.logger.info('stop signal is occured.')
        ml_processes = self.processes.filter(
            experiment_process_type__in=[
                EXPERIMENT_PROCESS_TYPE.optuna,
                EXPERIMENT_PROCESS_TYPE.ensemble,
            ],
            finished_at__isnull=True).all()
        for proc in ml_processes:
            proc.stop()

        # 한번 stop하면 수행하지 않아야 하지만; 종종 재시도해볼 경우가 있다. stop동작이 조금 불완전함.
        # container_ids = self.containers.all().values_list('container_id', flat=True)
        # if container_ids:
        #     client = docker.from_env()
        #     for container_id in container_ids:
        #         container = client.containers.get(container_id)
        #         container.kill()
        # pod_names = self.containers.all().values_list('pod_name', flat=True)
        # if pod_names:
        #     for pod_name in pod_names:
        #         PodExt.delete(pod_name, body={
        #             'kind': 'DeleteOptions',
        #             'apiVersion': 'v1',
        #             'propagationPolicy': 'Foreground'
        #         })

    @property
    def total_slot(self):
        '''seconds to work'''
        if self.started_at is None:
            return self.timeout
        else:
            try:
                should_finish_at = max(self.processes.filter(
                    should_finish_at__isnull=False).values_list('should_finish_at', flat=True))
            except ValueError:
                should_finish_at = None
            else:
                if should_finish_at:
                    return (should_finish_at - self.started_at).seconds

        return self.timeout

    @property
    def done_slot(self):
        if self.started_at is None:
            return 0

        if self.stopped_at:
            first = self.stopped_at
        else:
            if self.status == EXPERIMENT_STATUS.learning:
                now = timezone.now()
                # 최대 timeout을 넘지않도록;
                if now > (self.started_at + timezone.timedelta(seconds=self.timeout)):
                    first = self.started_at + timezone.timedelta(seconds=self.timeout)
                else:
                    first = now
            else:
                # TODO: 종료되면 처리한 시간이 아닌 전체시간이 done으로 return되기때문에 개선필요;
                return self.timeout
        return (first - self.started_at).seconds

    def make_a_copy(self, deployment_id=None):
        """현재 experiment를 copy 합니다. deployment_id를 parameter로 받을 경우 copy된 deployment_id도 같이 return 합니다. """
        new_deployment = None
        if deployment_id:
            from accutuning_validator.models import Deployment
            old_deployment = Deployment.objects.filter(experiment__id=self.id).get(pk=deployment_id)
            if not old_deployment:
                raise ValueError(f'deployment_id {deployment_id}이(가) experiment(id:{self.id})에 없습니다.')

            new_deployment = old_deployment
            new_deployment.pk = None
            new_deployment.save()

        new_experiment = Experiment.objects.get(pk=self.id)
        if new_experiment.name == f'Experiment-{self.id}':
            new_experiment.name = None
        new_experiment.pk = None
        new_experiment.id = None
        new_experiment.original_id = self.id
        new_experiment.copied = False

        new_experiment.save()

        from ..tasks import copy_experiment_async
        copy_experiment_async.delay(self.id, new_experiment.id, 'experiment', deployment_id, new_deployment.id if new_deployment else None)

        return new_experiment, new_deployment

    def _make_a_copy(self, new_experiment_id, old_deployment_id=None, new_deployment_id=None):
        """celery에서 호출, copy 할 experiment(old_experiment)에서 실행될 예정"""
        from accutuning_preprocessor.models import Dataset, Column
        from accutuning_validator.models import Deployment
        import itertools

        new_experiment = Experiment.objects.get(pk=new_experiment_id)

        self.logger.info(f'copy from {self.name} to {new_experiment.name}')

        def change_path(model_obj, col_names):
            self._replace_content_in_columns(model_obj, col_names, self.workspace_name, new_experiment.workspace_name)

        change_path(new_experiment, ['pycaret_conffile', 'pycaret_setupfile'])

        self.logger.info('copy Dataset, Column Objects')
        new_datasets = Dataset.objects.filter(experiment__id=self.id)
        for new_dataset in new_datasets:
            dataset_old_id = new_dataset.id  # Column 객체를 Copy하기 위해.
            new_dataset.pk = None
            new_dataset.experiment = new_experiment

            new_dataset.save()

            change_path(new_dataset, ['preprocessor_file', 'recommender_file'])

            new_columns = Column.objects.filter(dataset__id=dataset_old_id)
            for new_column in new_columns:
                new_column.pk = None
                new_column.dataset = new_dataset
                new_column.save()

        self.logger.info('copy Model Objects')
        model_mapping = {}  # model key copy하며 old와 new 매핑 -> Deployment Copy할 때 이용하기 위해.
        new_mlmodels = MlModel.objects.filter(experiment__id=self.id)
        new_mlensembles = MlEnsemble.objects.filter(experiment__id=self.id)
        new_models = itertools.chain(new_mlmodels, new_mlensembles)
        for new_model in new_models:
            model_old_id = new_model.id
            new_model.pk = None
            new_model.id = None
            new_model.experiment = new_experiment
            new_model.save()

            model_mapping[model_old_id] = new_model.id

            change_path(new_model, ['file', 'reprfile'])

        self.logger.info('copy Deployment Objects')

        if new_deployment_id:
            new_deployments = Deployment.objects.filter(id=new_deployment_id)  # 아래 for 로직을 같이 사용하기 위해 get() 대신 filter() 사용
        else:
            new_deployments = Deployment.objects.filter(experiment__id=self.id)

        for new_dep in new_deployments:
            if new_dep.id != new_deployment_id:
                new_dep.pk = None

            # new_deployment.experiment = new_experiment  # Model을 바꾸면 자동으로 바뀜
            new_dep.model_pk = model_mapping[new_dep.model_pk]
            new_dep.save()

            change_path(new_dep, ['model_fp', 'pipeline_fp', 'runtime_model_fp', 'runtime_model_dir'])

        self._copy_workspace_to_other(new_experiment)

        new_experiment.copied = True
        new_experiment.save(update_fields=['copied'])

        self.logger.info(f'copy from {self.name} to {new_experiment.name} complete!')


class ExperimentProcess(ModelWorkspaceMixin, ExportableModelMixin, K8sContainerMixin, MultiprocessMixin, ExperimentContainerCtlMixin, models.Model):
    experiment = models.ForeignKey(
        ExperimentBase, on_delete=models.CASCADE,
        related_name='processes')
    experiment_process_type = models.CharField(max_length=255, choices=EXPERIMENT_PROCESS_TYPE.choices(), help_text='실험작업 타입(parse, prerprocess, cluster, dataset_eda, optuna, ensemble, modelstat, deploy, predicr, labeling 중 택1')
    experiment_target = models.IntegerField(null=True, help_text='experiment 작업 타입. 타입에 따라 대상pk를 지정할 수 있습니다.')
    experiment_target_type = models.CharField(max_length=255, null=True, help_text='experiment 유형에 따라 지정된 대상pk의 class type')
    # use_parallel = models.BooleanField(default=False)
    estimators_chunk_json = models.TextField(null=True) #TODO: (질문) 이 두개는 무슨 변수인가요?
    extra_request_conf = models.FileField(max_length=1024, null=True)
    output_file_prefix = models.CharField(max_length=25, null=True, help_text='생성되는 중간파일들의 prefix')
    proceed_next = models.BooleanField(
        default=False,
        help_text='후속단계가 있다면 진행합니다.'
    )
    created_at = models.DateTimeField(auto_now_add=True, help_text='작업 생성 시간')
    started_at = models.DateTimeField(null=True, blank=True, help_text='작업 시작 시간')
    stopped_at = models.DateTimeField(null=True, blank=True, help_text='작업 정지 요청 시간')
    finished_at = models.DateTimeField(null=True, blank=True, help_text='작업 종료 시간')
    should_finish_at = models.DateTimeField(null=True, blank=True, help_text='작업 끝나야하는 시간(deprecated)')
    use_container = models.BooleanField(default=False, help_text='수행시 container 생성여부')
    input_args_json = models.TextField(null=True, blank=True, help_text='입력 arguments')

    stop_request = models.BooleanField(default=False, help_text='frontend에서 작업 정지 요청여부')
    stop_reponse = models.BooleanField(default=False, help_text='worker에서 작업 정지 요청여부')

    host = models.CharField(null=True, blank=True, max_length=255)
    pid = models.IntegerField(null=True)
    killed = models.BooleanField(default=False)

    container_uuid = models.CharField(max_length=255, null=True, help_text='컨테이너 고유ID')
    error = models.BooleanField(default=False, help_text='에러여부')
    error_message = models.TextField(null=True, blank=True, help_text='에러 메시지')
    error_code = models.IntegerField(null=True, help_text='에러 코드')

    def __str__(self):
        return f'pk:{self.experiment.pk}-typ:{self.experiment_process_type}-uuid:{self.container_uuid}'

    @property
    def current_trial(self):
        confp = Path(self.workspace_path, 'intermediate')
        try:
            infofp = next(iter(sorted(
                confp.glob(f'{self.output_file_prefix}_*.start'),
                reverse=True)))
        except StopIteration:
            return None
        try:
            ret = json.loads(infofp.read_text())
        except IOError:
            return None

        try:
            ret['elapsed_sec'] = time.time() - ret['start']
        except KeyError:
            ret['elapsed_sec'] = None
        return ret

    @property
    def default_docker_image_name(self):
        return AccutuningSiteConfiguration.proc(
            self.experiment_process_type, 'image_name')

    @property
    def default_docker_workspace_base(self):
        return '/workspace'

    @property
    def default_docker_workspace_path(self):
        return os.path.join(
            self.default_docker_workspace_base,
            self.workspace_name
        )

    @property
    def default_docker_volume_mount_map(self):
        # ACCUTUNING_K8S_VOLUME_MOUNT_HOST_PATH 는 local환경에서 의미있는 경로로 전달해야 함
        # 결국 docker command를 local에서 수행하기 때문에
        if settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH:
            source_path = os.path.join(
                settings.ACCUTUNING_K8S_VOLUME_MOUNT_PATH,
                # self.experiment.workspace_name,
            )
        else:
            source_path = self.experiment.workspace_path
        return ':'.join([
            source_path,
            os.path.join(
                self.default_docker_workspace_base,
                # self.experiment.workspace_name,
            ),
        ])

    @property
    def default_docker_command(self):
        docker_image_name = self.default_docker_image_name
        volumes_maps = [
            self.default_docker_volume_mount_map
        ]
        volumes_maps.extend(settings.ACCUTUNING_DOCKER_VOLUME_MOUNT_MAPS)
        # volumne_map_strs = ['-v {}:{} \\'.format(
        #     volumes_map[0],
        #     volumes_map[1],
        # )]
        volumne_map_strs = []
        for vol in volumes_maps:
            try:
                source, target = vol.split(':')
                volumne_map_strs.append('-v {}:{} \\'.format(
                    source, target,
                ))
            except ValueError:
                pass

        return '''docker run -it \\
-e ACCUTUNING_LOG_LEVEL={} \\
-e ACCUTUNING_WORKSPACE={} \\
{}
{}'''.format(
            self.experiment.log_level,
            self.default_docker_workspace_path,
            '\n'.join(volumne_map_strs),
            docker_image_name,
        )

    def start(self):
        self.experiment.logger.info('start {} {}'.format(self.experiment_process_type, self.container_uuid))
        _start = self._start_process
        _start(
            experiment_process_type=self.experiment_process_type,
            experiment_uuid=self.container_uuid,
        )

    def stop(self):
        if self.stop_request is False:
            self.experiment.logger.info('stopped(request)')
            self.stop_request = True
            self.stopped_at = timezone.now()
            self.save(update_fields=['stop_request', 'stopped_at'])
        else:
            self.experiment.logger.info('already stopped process')

    @property
    def conf(self):
        p = Path(self.workspace_path, 'conf.pkl')
        try:
            import pickle
            return pickle.loads(p.read_bytes())
        except Exception as e:
            return dict(_error=str(e))


class ModelBase(models.Model):
    objects = InheritanceManager()

    created_at = models.DateTimeField(auto_now_add=True, help_text='모델 생성 시간')
    filesize = models.IntegerField(null=True, help_text='모델 파일 크기')
    file = models.FileField(max_length=1024, null=True, help_text='모델 binary 파일')
    reprfile = models.FileField(max_length=1024, null=True)
    score = models.FloatField(null=True)
    train_score = models.FloatField(null=True)
    valid_score = models.FloatField(null=True)
    test_score = models.FloatField(null=True)
    estimator_name = models.CharField(null=True, max_length=100)
    dummy = models.BooleanField(default=False)
    deployed = models.BooleanField(default=False, help_text='배포 여부')
    experiment = models.ForeignKey('ExperimentBase', on_delete=models.CASCADE)
    generator = models.CharField(null=True, blank=True, max_length=100, help_text='모델을 생성한 automl서비스이름(autosklearn, tpot)')
    fitted = models.BooleanField(default=False, help_text='최적화 여부')

    evaluation_stat_status = models.CharField(choices=LONG_TASK_STATUS.choices(), max_length=15, default='INIT', help_text='모델 평가용 통계정보 요청 상태')
    all_metrics_status = models.CharField(choices=LONG_TASK_STATUS.choices(), max_length=15, default='INIT', help_text='모델 metric 요청 상태')
    all_metrics_json = models.TextField(null=True, blank=True, help_text='모델의 전체 metric(json)')
    searchspace_json = models.TextField(null=True, blank=True, help_text='모델 상세')
    plot_choices_json = models.TextField(null=True, blank=True, help_text='선택가능한 플롯목록')

    class Meta:
        ordering = ['experiment', '-score', 'id']

    @property
    def pipeline_info(self):
        return self.objects.get_subclass().pipeline_info

    @property
    def deployed_status(self):
        from accutuning_validator.models import Deployment
        dep = Deployment.objects.filter(model_pk=self.pk, experiment=self.experiment).order_by('-pk').first()
        if dep:
            return dep.status
        else:
            return None

    @property
    def available_plot_choices(self):
        if ExperimentBase.objects.get_subclass(id=self.experiment.id).estimator_type == ESTIMATOR_TYPE.classifier:
            return [
                'permutation_features',
                'auc',
                'threshold',
                'pr',
                'confusion_matrix',
                'error',
                'class_report',
                'boundary',
                'rfe',
                'learning',
                'manifold',
                'calibration',
                'vc',
                'dimension',
                'feature',
                'feature_all',
                'lift',
                'gain',
                'tree',
            ]
        elif ExperimentBase.objects.get_subclass(id=self.experiment.id).estimator_type == ESTIMATOR_TYPE.regressor:
            return [
                'permutation_features',
                'residuals',
                'error',
                'cooks',
                'rfe',
                'learning',
                'vc',
                'manifold',
                'feature',
                'feature_all',
                'tree',
            ]
        else:
            return []

    @property
    def plot_choices(self):
        try:
            return json.loads(self.plot_choices_json)
        except (ValueError, TypeError):
            # default
            if ExperimentBase.objects.get_subclass(id=self.experiment.id).estimator_type == ESTIMATOR_TYPE.classifier:
                return [
                    'permutation_features',
                    # 'auc',
                    # 'threshold',
                    # 'pr',
                    # 'confusion_matrix',
                    # 'error',
                    # 'class_report',
                    # 'boundary',
                    # 'rfe',
                    # 'learning',
                    # 'manifold',
                    # 'calibration',
                    # 'vc',
                    # 'dimension',
                    # # 'feature',
                    # # 'feature_all',
                    # # 'lift',
                    # # 'gain',
                    # # 'tree',
                ]
            elif ExperimentBase.objects.get_subclass(id=self.experiment.id).estimator_type == ESTIMATOR_TYPE.regressor:
                return [
                    'permutation_features',
                    # 'residuals',
                    # 'error',
                    # 'cooks',
                    # 'rfe',
                    # 'learning',
                    # 'vc',
                    # 'manifold',
                    # # 'feature',
                    # # 'feature_all',
                    # # 'tree',
                ]
            else:
                return []

    @property
    def plots(self):
        return (
            MlModelVisualizedObject.objects.filter(
                model=self).values_list(
                    'name', flat=True)
        )


class MlModelVisualizedObject(VisualizedObject):
    model = models.ForeignKey(
        ModelBase,
        related_name='visualized_objects',
        on_delete=models.CASCADE)


class MlModel(MlModelEvaluationMixin, ModelBase):

    @property
    def codestr(self):
        from accutuning.helpers.sklearncodebuilder import build_pipeline_code
        return build_pipeline_code(self.pipeline)

    def __str__(self):
        return f'{self.estimator_name}(pk:{self.pk}, loss:{self.score}, gen:{self.generator})'

    @property
    def pipeline_info(self):
        if self.reprfile:
            return json.load(self.reprfile.open('rt'))
        else:
            return []

    class Meta:
        ordering = ['experiment', '-score', 'id']


class MlEnsemble(MlEnsembleEvaluationMixin, ModelBase):
    @property
    def pipeline_info(self):
        if self.reprfile:
            steps_with_weight = json.load(self.reprfile.open('rt'))
            return [
                (
                    str(weight), pipeline
                )
                for (weight, pipeline) in steps_with_weight
            ]
        else:
            return []


class MlCluster(models.Model):
    experiment = models.ForeignKey('Experiment', related_name='cluster_results', on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    input_file = models.FileField(max_length=1024, null=True)
    output_file = models.FileField(max_length=1024, null=True)
    cluster_json_file = models.FileField(max_length=1024, null=True)
    cluster_pickle_file = models.FileField(max_length=1024, null=True)


class MlClusterVisualizedObject(VisualizedObject):
    cluster = models.ForeignKey(
        MlCluster,
        related_name='visualized_objects',
        on_delete=models.CASCADE)


class MlClusterModel(ModelBase):

    class Meta:
        ordering = ['experiment', '-score', 'id']
