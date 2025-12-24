import cartopy.io.shapereader as shpreader

def download_maps():
    print("Pre-downloading map features...")
    
    resolutions = ['10m', '50m', '110m']
    features = [
        ('physical', 'coastline'),
        ('cultural', 'admin_0_boundary_lines_land')
    ]
    
    for res in resolutions:
        for category, name in features:
            try:
                path = shpreader.natural_earth(resolution=res, category=category, name=name)
                print(f"Downloaded {res} {category}/{name} to {path}")
            except Exception as e:
                print(f"Failed to download {res} {category}/{name}: {e}")

if __name__ == "__main__":
    download_maps()
