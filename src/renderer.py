import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.util import add_cyclic_point
import os
import numpy as np

class Renderer:
    def __init__(self):
        pass

    def warm_up(self, grib_path):
        """
        Opens the GRIB file with various filters to ensure index files are created
        sequentially before parallel processing.
        """
        print(f"Warming up GRIB index for {grib_path}...")
        try:
            # t2m
            xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'shortName': '2t'}}).close()
            # apcp
            xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'shortName': 'tp'}}).close()
            # synoptic - gh
            xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'isobaricInhPa', 'level': 500, 'shortName': 'gh'}}).close()
            # synoptic - t
            xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'isobaricInhPa', 'level': 500, 'shortName': 't'}}).close()
            # synoptic - prmsl
            xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}}).close()
            print("GRIB index warm-up complete.")
        except Exception as e:
            print(f"Warning: Failed to warm up GRIB index: {e}")

    def generate_map(self, grib_path, output_path, parameter="t2m", region_bounds=None, model="gfs"):
        """
        Generates a map from a GRIB file.
        
        Args:
            grib_path: Path to the input GRIB2 file.
            output_path: Path where the generated PNG should be saved.
            parameter: The parameter to plot (e.g., 't2m' for 2m temperature).
            region_bounds: Dictionary with lon_min, lon_max, lat_min, lat_max.
            model: The model name ('gfs' or 'ecmwf').
        """
        print(f"Generating map for {parameter} from {grib_path} (model: {model})...")
        
        try:
            # Determine projection
            if region_bounds:
                central_lon = (region_bounds['lon_min'] + region_bounds['lon_max']) / 2
                central_lat = (region_bounds['lat_min'] + region_bounds['lat_max']) / 2
                projection = ccrs.LambertConformal(central_longitude=central_lon, central_latitude=central_lat)
            else:
                projection = ccrs.PlateCarree()

            # Plotting
            plt.figure(figsize=(24, 18))
            ax = plt.axes(projection=projection)
            
            if region_bounds:
                ax.set_extent([
                    region_bounds['lon_min'], region_bounds['lon_max'],
                    region_bounds['lat_min'], region_bounds['lat_max']
                ], crs=ccrs.PlateCarree())
            
            # Add map features
            ax.add_feature(cfeature.COASTLINE, linewidth=1.5)
            ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=1)
            
            if parameter == "t2m":
                ds = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'shortName': '2t'}})
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
                ds = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'shortName': 'tp'}})
                # Use specific levels to make low precipitation visible and a high-contrast colormap
                levels = [0.2, 0.5, 1, 2, 5, 10, 20, 30, 40, 50, 75, 100]
                
                # Add cyclic point to avoid white line at Greenwich
                tp = ds['tp']
                data = tp.values
                
                # Convert units if necessary
                if model == 'ecmwf':
                    # ECMWF tp is in meters, convert to mm (kg/m^2)
                    data = data * 1000.0
                
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
            elif parameter == "synoptic":
                # 500 hPa Geopotential Height
                ds_hgt = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'isobaricInhPa', 'level': 500, 'shortName': 'gh'}})
                hgt = ds_hgt['gh']
                
                # 500 hPa Temperature
                ds_tmp = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'isobaricInhPa', 'level': 500, 'shortName': 't'}})
                tmp = ds_tmp['t'] - 273.15 # Convert to Celsius
                
                # MSLP
                if model == 'ecmwf':
                    ds_prmsl = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'meanSea', 'shortName': 'msl'}})
                    prmsl = ds_prmsl['msl'] / 100.0 # Convert to hPa
                else:
                    ds_prmsl = xr.open_dataset(grib_path, engine='cfgrib', backend_kwargs={'filter_by_keys': {'typeOfLevel': 'meanSea', 'shortName': 'prmsl'}})
                    prmsl = ds_prmsl['prmsl'] / 100.0 # Convert to hPa
                
                # Prepare data for plotting (cyclic point)
                lons = hgt.longitude.values
                lats = hgt.latitude.values
                
                hgt_c, lons_c = add_cyclic_point(hgt.values, coord=lons)
                tmp_c, _ = add_cyclic_point(tmp.values, coord=lons)
                prmsl_c, _ = add_cyclic_point(prmsl.values, coord=lons)
                
                # Plot Geopotential Height (Color fill)
                # Levels similar to the image (476 to 600 gpdm -> 4760 to 6000 gpm)
                hgt_c_gpdm = hgt_c / 10.0
                levels_hgt = np.arange(480, 600, 4) # 4 gpdm interval
                
                im = ax.contourf(lons_c, lats, hgt_c_gpdm, transform=ccrs.PlateCarree(), cmap='jet', levels=levels_hgt, extend='both')
                plt.colorbar(im, ax=ax, label='500 hPa Geopotential Height (gpdm)')
                
                # Highlight 552 gpdm line
                cs_552 = ax.contour(lons_c, lats, hgt_c_gpdm, transform=ccrs.PlateCarree(), colors='black', levels=[552], linewidths=2)
                ax.clabel(cs_552, inline=True, fmt='%d', fontsize=10)
                
                # Plot MSLP (White contours)
                levels_prmsl = np.arange(900, 1100, 5) # 5 hPa interval
                cs_prmsl = ax.contour(lons_c, lats, prmsl_c, transform=ccrs.PlateCarree(), colors='white', levels=levels_prmsl, linewidths=1.5)
                ax.clabel(cs_prmsl, inline=True, fmt='%d', fontsize=10)
                
                # Plot 500 hPa Temperature (Dashed contours)
                levels_tmp = np.arange(-50, 20, 5) # 5 deg C interval
                cs_tmp = ax.contour(lons_c, lats, tmp_c, transform=ccrs.PlateCarree(), colors='grey', levels=levels_tmp, linestyles='dashed', linewidths=1)
                ax.clabel(cs_tmp, inline=True, fmt='%d', fontsize=8)
                
                plt.title("500 hPa Geopot. (gpdm), T (C), MSLP (hPa)")

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
