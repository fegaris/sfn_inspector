import boto3
import csv
import re
from datetime import datetime


def list_executions(state_machine_arn, status):
    # Crear una sesión de boto3
    session = boto3.Session()

    # Crear un cliente para AWS Step Functions
    sfn_client = session.client('stepfunctions')

    # Obtener todas las ejecuciones de la máquina de estado especificada
    executions = []
    response = sfn_client.list_executions(stateMachineArn=state_machine_arn, maxResults=1000, statusFilter=status)

    # Paginar a través de todas las ejecuciones si hay más de una página de resultados
    while True:
        executions.extend(response['executions'])
        if 'nextToken' in response:
            response = sfn_client.list_executions(stateMachineArn=state_machine_arn, nextToken=response['nextToken'],
                                                  maxResults=1000, statusFilter=status)
        else:
            break

    return executions


def extract_execution_id(execution_arn):
    # Usar una expresión regular para extraer la parte específica del ARN
    match = re.search(r'([^:]+)$', execution_arn)
    if match:
        return match.group(1).split('_')[0]
    return None


def save_executions_to_csv(executions, file_name):
    # Definir los encabezados del archivo CSV
    headers = ['Execution ARN', 'Status', 'Start Date', 'Stop Date', 'ID']

    # Escribir los datos en un archivo CSV
    with open(file_name, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        for exec in executions:
            execution_id = extract_execution_id(exec['executionArn'])
            writer.writerow([
                exec['executionArn'],
                exec['status'],
                exec['startDate'],
                exec.get('stopDate', 'N/A'),
                execution_id
            ])


def main():
    # Reemplaza esto con el ARN de tu máquina de estado
    state_machine_arn = 'arn:aws:states:region:account-id:stateMachine:stateMachineName'

    executions = list_executions(state_machine_arn, 'FAILED')

    for exec in executions:
        print(f"Execution ARN: {exec['executionArn']}")
        print(f"Status: {exec['status']}")
        print(f"Start Date: {exec['startDate']}")
        if 'stopDate' in exec:
            print(f"Stop Date: {exec['stopDate']}")
        execution_id = extract_execution_id(exec['executionArn'])
        print(f"ID: {execution_id}")
        print("------------------------------------------------------")

    # Guardar los resultados en un archivo CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'step_functions_executions_{timestamp}.csv'
    save_executions_to_csv(executions, file_name)
    print(f"Data saved to {file_name}")


if __name__ == "__main__":
    main()
