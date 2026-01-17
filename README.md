# CP431
Parallel Programming Using the Message Passing Interface (MPI) with MPI4PY

## Virtual Environment Setup and Dependency Installation

### 1. Create a Virtual Environment
```bash
python3 -m venv venv
```

### 2. Activate the Virtual Environment
- **On Linux/macOS:**
  ```bash
  source venv/bin/activate
  ```
- **On Windows:**
  ```bash
  venv\Scripts\activate
  ```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

You can verify the installation by checking that `mpi4py` is installed:
```bash
python3 -c "from mpi4py import MPI; print('MPI4PY installed successfully')"
```

## Running MPI Programs

To run MPI code, use the standardized command format:

```bash
mpirun -np <num_processes> python3 <path_to_script>
```

### Example: Running the Point-to-Point Communication Program

```bash
mpirun -np 4 python3 A1/point-to-point.py
```

- `-np 4` specifies the number of processes to create
- Adjust the number to run with different numbers of processes as needed

### Authors
Vicky Sekhon, Marko Misic, Cassel Robson, Maathusan Raveendran