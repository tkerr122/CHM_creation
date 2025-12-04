# Theo Kerr on 11/20/2025

# Imports
import pylasr  # type: ignore
import argparse, os


# =========================
# Define custom functions
# =========================
def create_CHM(input_dir, output_file, cores):
    try:
        ### STEP 1: Create the pipeline
        # Load in point cloud
        las = pylasr.reader_coverage() 
        info = pylasr.info()
        
        # Delete noise points
        low_noise = pylasr.delete_points(filter=["Classification == 7"])
        high_noise = pylasr.delete_points(filter=["Classification == 18"])
        
        # Delete points above 50 meters
        above_threshold = pylasr.delete_points(filter=["Z > 50"]) 
        
        # Create DTM
        dtm = pylasr.triangulate(filter=["Classification == 2"]) 
        
        # Normalize the point cloud
        nlas = pylasr.transform_with(dtm, operation="-") 
        
        # Create CHM
        chm = pylasr.rasterize(res=4, window=2.0, operators=["max"], ofile=output_file) 
        
        ### STEP 2: Execute the pipeline
        # pipeline = las + low_noise + high_noise + above_threshold + dtm + nlas + chm
        # pipeline.set_concurrent_files_strategy(ncores=cores)
        # pipeline.set_verbose(False)
        # pipeline.set_progress(True)
        pipeline = las + info 
        pipeline.set_sequential_strategy()
        # pipeline.set_concurrent_files_strategy(ncores=cores)
        pipeline.set_verbose(True)
        result = pipeline.execute(input_dir)

        if result['success']:
            print("CHM processing successful")
            print(f"Data: {result['data']}")
            
        else:
            print(f"Error: {result['message']}")
    
    except Exception as e:
        print(f"Exception: {e}")


# =========================
# Create the CHMs
# =========================
def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description="Script for creating CHMs")
    parser.add_argument("-s", "--survey", type=str, help="Survey name", required=True)
    parser.add_argument("-c", "--cores", type=int, default=30, help="Number of cores to use for processing")
    args = parser.parse_args()

    # Set up environment
    survey = args.survey
    cores = args.cores
    input_dir = os.path.join("/gpfs/glad1/Theo/Data/Lidar/LAZ", survey)
    output_dir = os.path.join("/gpfs/glad1/Theo/Data/Lidar/CHMs_raw", survey)
    output_file = os.path.join(output_dir, f"{survey}.tif")
    os.makedirs(output_dir, exist_ok=True)
    
    # Create CHMs
    create_CHM(input_dir, output_file, cores)

if __name__ == "__main__":
    main()