# 2nd Airflow Warsaw Meetup - One DAG to rule them all

Materials related to my talk on 2nd Warsaw Apache Airflow Meetup

```
class TriggeredDagLink(BaseOperatorLink):
    """
    Link to the triggered dag
    """

    name = "Go to the DAG"

    # Assuming operator of class SubDagOperator
    def get_link(self, operator, dttm):
        return "{}/graph?dag_id={}".format(get_airflow_link(), operator.task_id)


class SubDagWithDagLinkOperator(SubDagOperator):
    operator_extra_links = (TriggeredDagLink(),)


def add_closing_block(dag):
    leaf_tasks = get_tasks_without_downstreams(dag)
    ending_task = dag_completed_slack_notification(dag)
    for task in leaf_tasks:
        task.set_downstream(ending_task)


def get_tasks_without_downstreams(dag):
    return [task for task in dag.tasks if not task.downstream_task_ids]


def dag_completed_slack_notification(dag):
    slack_msg = """
        :heavy_check_mark: Workflow successfully completed!
        *DAG*: {dag}
        """.format(
        dag=dag.dag_id
    )
    slack_notification = SlackWebhookOperator(
        task_id="slack_complete_notification",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=...,
        message=slack_msg,
        channel="#channel",
        dag=dag,
    )
    return slack_notification


def task_fail_slack_alert(context):
    dag = context.get("dag")
    task_instance = context.get("task_instance")
    slack_msg = """
        :red_circle: Task Failed.
        *DAG*: {dag}
        *Task*: {task}
        *Execution Time*: {exec_date}
        *Logs*: <{log_url}|click_here>
        """.format(
        dag=dag.dag_id,
        task=task_instance.task_id,
        exec_date=context.get("execution_date"),
        log_url=task_instance.log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        http_conn_id=SLACK_CONN_ID,
        webhook_token=...,
        message=slack_msg,
        channel="#channel",
        dag=dag,
    )
    return failed_alert.execute(context=context)
```

```
class CustomExternalTaskSensor(BaseSensorOperator):

    template_fields = ["external_dag_id", "external_task_id"]
    ui_color = "#19647e"

    @apply_defaults
    def __init__(
        self,
        external_dag_id,
        external_task_id,
        allowed_states=None,
        execution_delta=None,
        execution_date_fn=None,
        last_run=False,
        last_run_after_this_dag_start=False,
        check_existence=False,
        *args,
        **kwargs
    ):
        super(CustomExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        if external_task_id:
            if not set(self.allowed_states) <= set(State.task_states):
                raise ValueError(
                    "Valid values for `allowed_states` "
                    "when `external_task_id` is not `None`: {}".format(
                        State.task_states
                    )
                )
        else:
            if not set(self.allowed_states) <= set(State.dag_states):
                raise ValueError(
                    "Valid values for `allowed_states` "
                    "when `external_task_id` is `None`: {}".format(State.dag_states)
                )

        if (
            execution_delta is not None
            and execution_date_fn is not None
            and (last_run is not None or last_run_after_this_dag_start is not None)
        ):
            raise ValueError(
                "Only one of `execution_delta` or `execution_date_fn` "
                "or `last_run` or/and `last_run_after_this_dag_start` may "
                "be provided to CustomExternalTaskSensor; not more."
            )

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.last_run = last_run
        self.last_run_after_this_dag_start = last_run_after_this_dag_start
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.check_existence = check_existence
        # we only check the existence for the first time.
        self.has_checked_existence = False

    @staticmethod
    def get_dag_by_id(dag_id: str):
        return DagBag().get_dag(dag_id)

    @provide_session
    def poke(self, context, session=None):
        if self.last_run or self.last_run_after_this_dag_start:
            external_dag = self.get_dag_by_id(self.external_dag_id)
            external_dag_latest_execution_date = external_dag.latest_execution_date
            if self.last_run_after_this_dag_start:
                while (
                    external_dag_latest_execution_date
                    < context.get("dag_run").execution_date
                ):
                    external_dag_latest_execution_date = (
                        external_dag.latest_execution_date
                    )  # latest_execution_date is a property refresh with every call
                    time.sleep(30)
            dttm = external_dag_latest_execution_date
        elif self.execution_delta:
            dttm = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self.execution_date_fn(context["execution_date"])
        else:
            dttm = context["execution_date"]

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ",".join(
            [datetime.isoformat() for datetime in dttm_filter]
        )

        self.log.info(
            "Poking for "
            "{self.external_dag_id}."
            "{self.external_task_id} on "
            "{} ... ".format(serialized_dttm_filter, **locals())
        )

        DM = DagModel
        TI = TaskInstance
        DR = DagRun

        # we only do the check for 1st time, no need for subsequent poke
        if self.check_existence and not self.has_checked_existence:
            dag_to_wait = (
                session.query(DM).filter(DM.dag_id == self.external_dag_id).first()
            )

            if not dag_to_wait:
                raise AirflowException(
                    "The external DAG "
                    "{} does not exist.".format(self.external_dag_id)
                )
            else:
                if not os.path.exists(dag_to_wait.fileloc):
                    raise AirflowException(
                        "The external DAG "
                        "{} was deleted.".format(self.external_dag_id)
                    )

            if self.external_task_id:
                refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(
                    self.external_dag_id
                )
                if not refreshed_dag_info.has_task(self.external_task_id):
                    raise AirflowException(
                        "The external task"
                        "{} in DAG {} does not exist.".format(
                            self.external_task_id, self.external_dag_id
                        )
                    )
            self.has_checked_existence = True

        if self.external_task_id:
            count = (
                session.query(TI)
                .filter(
                    TI.dag_id == self.external_dag_id,
                    TI.task_id == self.external_task_id,
                    TI.state.in_(self.allowed_states),
                    TI.execution_date.in_(dttm_filter),
                )
                .count()
            )
        else:
            count = (
                session.query(DR)
                .filter(
                    DR.dag_id == self.external_dag_id,
                    DR.state.in_(self.allowed_states),
                    DR.execution_date.in_(dttm_filter),
                )
                .count()
            )

        session.commit()
        return count == len(dttm_filter)
```
