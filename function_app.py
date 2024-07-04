import logging
import azure.functions as func
import modules.task

app = func.FunctionApp()

@app.schedule(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=True) 
def func_netsuite_frs(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function executed.')

    modules.task.main()
    logging.info('func_netsuite_frs function executed.')
 
