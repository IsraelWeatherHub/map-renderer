import redis
import json
import os
import config
from renderer import Renderer
from storage import Storage

def main():
    print("Starting MapRenderer Service...")
    
    # Connect to Redis
    r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe('grib.downloaded')
    
    renderer = Renderer()
    storage = Storage()
    
    print("Listening for 'grib.downloaded' events...")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
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
                parameters = ["t2m", "apcp"]
                
                for param in parameters:
                    # Generate local output path
                    output_filename = f"{param}_{forecast_hour}.png"
                    output_path = os.path.join("/tmp", output_filename)
                    
                    # Render
                    renderer.generate_map(grib_path, output_path, parameter=param)
                    
                    # Upload to MinIO
                    # Key structure: {model}/{run_date}/{run_hour}/{parameter}/{forecast_hour}.png
                    object_name = f"{model}/{run_date}/{run_hour}/{param}/{forecast_hour}.png"
                    storage.upload_file(output_path, object_name)
                    
                    # Notify completion (optional, for API to clear cache etc)
                    r.publish('maps.generated', json.dumps({
                        "model": model,
                        "run_date": run_date,
                        "run_hour": run_hour,
                        "parameter": param,
                        "forecast_hour": forecast_hour,
                        "url": object_name
                    }))
                    
            except Exception as e:
                print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()
