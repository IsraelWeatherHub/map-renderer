import pika
import json
import os
import time
import config
import concurrent.futures
from renderer import Renderer
from storage import Storage

# Global pool for map rendering (CPU bound)
map_renderer_pool = None

def process_task(grib_path, output_path, param, bounds, model, run_date, run_hour, forecast_hour, region_name):
    try:
        renderer = Renderer()
        storage = Storage()
        
        # Render
        renderer.generate_map(grib_path, output_path, parameter=param, region_bounds=bounds, model=model)
        
        # Upload to MinIO
        # Key structure: {model}/{run_date}/{run_hour}/{parameter}/{forecast_hour}_{region}.png
        object_name = f"{model}/{run_date}/{run_hour}/{param}/{forecast_hour}_{region_name}.png"
        storage.upload_file(output_path, object_name)
        
        return {
            "model": model,
            "run_date": run_date,
            "run_hour": run_hour,
            "parameter": param,
            "forecast_hour": forecast_hour,
            "region": region_name,
            "url": object_name
        }
    except Exception as e:
        print(f"Error processing task for {param} {region_name}: {e}")
        raise e

def handle_grib_task(body):
    global map_renderer_pool
    try:
        data = json.loads(body)
        print(f"Processing GRIB task in background: {data}")
        
        grib_path = data['file_path']
        model = data['model']
        run_date = data['run_date']
        run_hour = data['run_hour']
        
        try:
            if model == 'gfs':
                forecast_hour = grib_path.split('.f')[-1]
            elif model == 'ecmwf':
                # ecmwf_ifs_0p25_20251225_00z_3h.grib2
                forecast_hour = grib_path.split('_')[-1].split('.')[0].replace('h', '')
                forecast_hour = f"{int(forecast_hour):03d}"
            else:
                forecast_hour = "000"
        except:
            forecast_hour = "000"

        # Warm up GRIB index sequentially to avoid race conditions in parallel processing
        print(f"Warming up GRIB index for {grib_path}...")
        warmup_renderer = Renderer()
        warmup_renderer.warm_up(grib_path)
        
        # Define parameters to generate
        parameters = ["t2m", "apcp", "synoptic"]
        
        futures = []
        for param in parameters:
            for region_name, bounds in config.REGIONS.items():
                # Generate local output path
                output_filename = f"{param}_{forecast_hour}_{region_name}.png"
                output_path = os.path.join("/tmp", output_filename)
                
                # Submit task to global process pool
                futures.append(map_renderer_pool.submit(
                    process_task,
                    grib_path, output_path, param, bounds,
                    model, run_date, run_hour, forecast_hour, region_name
                ))
        
        # Establish a connection for publishing results
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=config.RABBITMQ_HOST, 
                    port=config.RABBITMQ_PORT,
                    heartbeat=600
                )
            )
            channel = connection.channel()
            channel.exchange_declare(exchange='weather_events', exchange_type='topic', durable=True)
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    message = future.result()
                    
                    # Notify completion
                    channel.basic_publish(
                        exchange='weather_events',
                        routing_key='map.generated',
                        body=json.dumps(message)
                    )
                    print(f"Published map.generated: {message}")
                except Exception as e:
                    print(f"Task failed: {e}")
            
            connection.close()
        except Exception as e:
            print(f"Error publishing results: {e}")

    except Exception as e:
        print(f"Error in handle_grib_task: {e}")

def main():
    print("Starting MapRenderer Service...")
    
    # Initialize global process pool
    global map_renderer_pool
    # Use a reasonable number of workers (e.g., CPU count)
    map_renderer_pool = concurrent.futures.ProcessPoolExecutor()
    
    # Thread pool for handling incoming GRIB messages concurrently
    grib_orchestrator_pool = concurrent.futures.ThreadPoolExecutor(max_workers=5)
    
    # Connect to RabbitMQ with retry
    connection = None
    while True:
        try:
            # Set a long heartbeat (600s = 10m) to avoid connection drop during long map generation
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=config.RABBITMQ_HOST, 
                    port=config.RABBITMQ_PORT,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            print("Connected to RabbitMQ")
            break
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ not ready, retrying in 5 seconds...")
            time.sleep(5)

    channel = connection.channel()
    
    # Declare exchange
    channel.exchange_declare(exchange='weather_events', exchange_type='topic', durable=True)
    
    # Determine which model to listen to
    listen_model = os.getenv("LISTEN_MODEL", "all")
    
    # Use a unique queue name based on the model to avoid conflicts
    queue_name = f'map_renderer_queue_{listen_model}'
    channel.queue_declare(queue=queue_name, durable=True)
    
    if listen_model == "all":
        channel.queue_bind(exchange='weather_events', queue=queue_name, routing_key='grib.downloaded.#')
    else:
        channel.queue_bind(exchange='weather_events', queue=queue_name, routing_key=f'grib.downloaded.{listen_model}')
        
    channel.queue_bind(exchange='weather_events', queue=queue_name, routing_key='map.deleted')
    
    storage = Storage()
    
    print(f"Listening for events on queue {queue_name} (model: {listen_model})...")
    
    def callback(ch, method, properties, body):
        try:
            if method.routing_key.startswith('grib.downloaded'):
                # Submit to thread pool for concurrent processing
                grib_orchestrator_pool.submit(handle_grib_task, body)
                
            elif method.routing_key == 'map.deleted':
                data = json.loads(body)
                print(f"Received delete request: {data}")
                object_name = data.get('url')
                if object_name:
                    storage.delete_file(object_name)
            
            # Auto ack is enabled, so no manual ack needed
        
        except Exception as e:
            print(f"Error processing message: {e}")

    # Enable auto_ack to allow fire-and-forget processing
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()
