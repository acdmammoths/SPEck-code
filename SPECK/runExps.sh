Help() {
   # Display Help
   echo "Use this script to replicate our experiments."
   echo
   echo "Syntax: runExps.sh [-d|-e|-h|-p]"
   echo "options:"
   echo "-d    Type of dataset. Must be one of the following:"
   echo "      sim     real    all"
   echo "-e    Type of experiment. Must be one of the following."
   echo "      runtime     sfsp    theta  all"
   echo "-p    Number of processors to use."
   echo "-h    Display this help message."
   echo
   echo "Note: SFSP experiments can only be run with real data."
}

while getopts e:d:p: flag ;do
    case "${flag}" in
        e) expType=${OPTARG};;
        d) dataset=${OPTARG};;
        p) numProcs=${OPTARG};;
    esac
done

while getopts :h option; do
   case $option in
      h) # display Help
         Help
         exit;;
   esac
done

realConfs () {
  java -cp target/SPEck-1.0-SNAPSHOT.jar CreateConfFiles "$numProcs"
}

simConfs() {
  # Generate simulated datasets

  mkdir -p data/sim_datasets
  #rm data/sim_datasets/*


  # Generate configuration files
  java -cp target/SPEck-1.0-SNAPSHOT.jar CreateConfFiles "$numProcs" -sim
}

# CHECK IF SFSP AND SIM

if [[ "$dataset" == "sim" && "$expType" != "runtime" ]]; then
  Help
  exit 1
fi

FOLDERS="confs/*"
mvn clean package

# Set up directories
rm -r confs
mkdir -p confs
mkdir -p results/csv

# --- REAL DATA --- #
if [[ "$dataset" == "real" ]]; then
  realConfs

# --- SIM DATA --- #
elif [[ "$dataset" == "sim" ]]; then
  simConfs

# --- ALL DATA --- #
elif [[ "$dataset" == "all" ]]; then
  realConfs
  simConfs
else
  Help
  exit 1
fi
mkdir -p results/csv
rm results/csv/*


# --- RUNTIME EXPERIMENTS --- #
if  [[ "$expType" == "runtime" || "$expType" == "all" ]]; then
  # Set up directory
  mkdir -p results/runtime
  rm results/runtime/*
  # Run experiment
  for conf in $FOLDERS; do
    echo "running RuntimeExperiment with ${conf}..."
    java -cp target/SPEck-1.0-SNAPSHOT.jar RuntimeExperiment $conf
    echo ""
  done
  java -cp target/SPEck-1.0-SNAPSHOT.jar ParseJsonFiles "./results/runtime" "./results/csv/runtimes.csv" "runtime"
  # Create plots
  mkdir -p plots
  if [[ "$dataset" == "real" ]]; then
    Rscript runtimeExps.R
  elif [[ "$dataset" == "sim" ]]; then
    Rscript runtimeExps.R -sim
  else
    Rscript runtimeExps.R
    Rscript runtimeExps.R -sim
  fi
fi
# --- SFSP EXPERIMENTS --- #
if [[ "$expType" == "sfsp" || "$expType" == "all" ]]; then
  # Set up directory
  mkdir -p results/sfsp
  rm results/sfsp/*

  # Run experiment
  for conf in $FOLDERS; do
    echo "running sfspExperiment with ${conf}..."
    java -cp target/SPEck-1.0-SNAPSHOT.jar SFSPExperiment $conf
    echo ""
  done
  java -cp target/SPEck-1.0-SNAPSHOT.jar ParseJsonFiles "./results/sfsp" "./results/csv/sfspData.csv" "sfsp"

  Rscript sfspExps.R sfspTable.csv
fi

# --- THETA EXPERIMENTS --- #
if [[ "$expType" == "theta" || "$expType" == "all" ]]; then
  echo "running thetaExperiment..."
  java -cp target/SPEck-1.0-SNAPSHOT.jar ThetaExperiment
  echo ""
  Rscript thetaExp.R

fi
# Remove excess files

rm data/*_mined*
rm data/*_random*
