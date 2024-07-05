import logging
import azure.functions as func
import task

app = func.FunctionApp()

@app.schedule(schedule="0 0 15 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=True) 
def func_netsuite_frs(myTimer: func.TimerRequest) -> None:
    task.main()
