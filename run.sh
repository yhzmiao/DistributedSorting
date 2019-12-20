echo "Loading Module..."
source loadModule.sh
echo "Checking Configure..."
g++ config.cpp -o config
./config
echo "Generating Data..."
source datagen.sh
echo "Compiling..."
mpiCC -mcmodel=large -O2 dsort.cpp -o dsort
echo "Running MPI..."
source mpirun.sh 
echo "Validating Data..."
source dataval.sh
