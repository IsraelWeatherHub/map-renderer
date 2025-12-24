import pika
import json
import os
import time
import config
from renderer import Renderer
from storage import Storage

def main():
    print("Starting MapRenderer Service...")
    
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
    
    # Declare queue and bind
    # Use a named queue 'map_renderer_queue' and make it durable so it survives restarts
    queue_name = 'map_renderer_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    
    channel.queue_bind(exchange='weather_events', queue=queue_name, routing_key='grib.downloaded')
    channel.queue_bind(exchange='weather_events', queue=queue_name, routing_key='map.deleted')
    
    renderer = Renderer()
    storage = Storage()
    
    print("Listening for events...")
    
    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            
            if method.routing_key == 'grib.downloaded':
                print(f"Received task: {data}")
                
                grib_path = data['file_path']
                model = data['model']
                run_date = data['run_date']
                run_hour = data['run_hour']
                
                # Extract forecast hour from filename (e.g., gfs.t12z.pgrb2.0p25.f003)
                # Simplified extraction
                try:
                    forecast_hour = grib_path.split('.f')[-1]
                except:
                    forecast_hour = "000"

                # Define parameters to generate
                parameters = ["t2m", "apcp", "synoptic"]
                
                for param in parameters:
                    for region_name, bounds in config.REGIONS.items():
                        # Generate local output path
                        output_filename = f"{param}_{forecast_hour}_{region_name}.png"
                        output_path = os.path.join("/tmp", output_filename)
                        
                        # Render
                        renderer.generate_map(grib_path, output_path, parameter=param, region_bounds=bounds)
                        
                        # Upload to MinIO
                        # Key structure: {model}/{run_date}/{run_hour}/{parameter}/{forecast_hour}_{region}.png
                        object_name = f"{model}/{run_date}/{run_hour}/{param}/{forecast_hour}_{region_name}.png"
                        storage.upload_file(output_path, object_name)
                        
                        # Notify completion
                        message = {
                            "model": model,
                            "run_date": run_date,
                            "run_hour": run_hour,
                            "parameter": param,
                            "forecast_hour": forecast_hour,
                            "region": region_name,
                            "url": object_name
                        }
                        ch.basic_publish(
                            exchange='weather_events',
                            routing_key='map.generated',
                            body=json.dumps(message)
                        )
                        print(f"Published map.generated: {message}")
                        
                        # Keep connection alive
                        connection.process_data_events()

            elif method.routing_key == 'map.deleted':
                print(f"Received delete request: {data}")
                object_name = data.get('url')
                if object_name:
                    storage.delete_file(object_name)
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        except pika.exceptions.AMQPError as e:
            print(f"Critical RabbitMQ error: {e}")
            # Re-raise to crash the script and trigger restart/reconnect
            raise e
        except Exception as e:
            print(f"Error processing message: {e}")
            # Optionally nack or reject, but for now we just log and maybe ack to avoid stuck messages
            # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()

if __name__ == "__main__":
    main()
