import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.util import add_cyclic_point
import os

class Renderer:
    def __init__(self):
        pass

    def generate_map(self, grib_path, output_path, parameter="t2m", region_bounds=None):
        """
        Generates a map from a GRIB file.
        
        Args:
            grib_path: Path to the input GRIB2 file.
            output_path: Path where the generated PNG should be saved.
            parameter: The parameter to plot (e.g., 't2m' for 2m temperature).
            region_bounds: Dictionary with lon_min, lon_max, lat_min, lat_max.
        """
        print(f"Generating map for {parameter} from {grib_path}...")
        
        try:
            # Open the dataset
            # filter_by_keys helps select specific messages if the file has many
            backend_kwargs = {}
            if parameter == "t2m":
                backend_kwargs = {'filter_by_keys': {'shortName': '2t'}}
            elif parameter == "apcp":
                backend_kwargs = {'filter_by_keys': {'shortName': 'tp'}}

            ds = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs=backend_kwargs)
            
            # Plotting
            plt.figure(figsize=(24, 18))
            ax = plt.axes(projection=ccrs.PlateCarree())
            
            if region_bounds:
                ax.set_extent([
                    region_bounds['lon_min'], region_bounds['lon_max'],
                    region_bounds['lat_min'], region_bounds['lat_max']
                ], crs=ccrs.PlateCarree())
            
            # Add map features
            ax.add_feature(cfeature.COASTLINE, linewidth=1.5)
            ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=1)
            
            if parameter == "t2m":
                # Convert Kelvin to Celsius
                temp_c = ds['t2m'] - 273.15
                
                # Add cyclic point to avoid white line at Greenwich
                data = temp_c.values
                lons = temp_c.longitude.values
                lats = temp_c.latitude.values
                data_c, lons_c = add_cyclic_point(data, coord=lons)
                
                im = ax.contourf(lons_c, lats, data_c, transform=ccrs.PlateCarree(), cmap='coolwarm', levels=100)
                plt.colorbar(im, ax=ax, label='Temperature (°C)')
                plt.title("2m Temperature (°C)")
            elif parameter == "apcp":
                # Use specific levels to make low precipitation visible and a high-contrast colormap
                levels = [0.2, 0.5, 1, 2, 5, 10, 20, 30, 40, 50, 75, 100]
                
                # Add cyclic point to avoid white line at Greenwich
                tp = ds['tp']
                data = tp.values
                lons = tp.longitude.values
                lats = tp.latitude.values
                data_c, lons_c = add_cyclic_point(data, coord=lons)
                
                im = ax.contourf(
                    lons_c, lats, data_c,
                    transform=ccrs.PlateCarree(), 
                    cmap='jet', 
                    levels=levels, 
                    extend='max'
                )
                plt.colorbar(im, ax=ax, label='Precipitation (kg/m^2)')
                plt.title("Total Precipitation (kg/m^2)")

            plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.1)
            plt.close()
            
            print(f"Map saved to {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Error generating map: {e}")
            # Fallback for testing if GRIB reading fails (e.g. if file is dummy or incomplete)
            self._create_dummy_image(output_path, f"Error: {e}")
            return output_path

    def _create_dummy_image(self, output_path, text):
        print("Creating dummy image due to error...")
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, text, ha='center', va='center', wrap=True)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        plt.savefig(output_path)
        plt.close()
