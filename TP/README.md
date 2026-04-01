# CP431 Term Project
Fractal Image Generation Using `mpi4py` and `OpenGL`.

## Authors
* Vicky Sekhon
* Cassel Robson
* Marko Misic
* Mathusan Raveendran

## Virtual Environment Setup and Dependency Installation

### 1. Create a Virtual Environment
```bash
python3 -m venv venv
```

### 2. Activate the Virtual Environment
- **On Unix**
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

## Commands
### Running Parallel Fractal Generation
- **On Unix**
```bash
mpirun -np <number-of-processors> python3 ./main.py -- <a+bj> <dimension-of-image>
```
- **On Windows**
```bash
mpiexec -n <number-of-processors> python3 ./main.py -- <a+bj> <dimension-of-image>
```
### Running Fractal Renderer
- **On Windows**
```bash
python main.py -- <a+bj> <dimension-to-rerender-fractal> <path-to-fractal-npy-array>
```

## Values of C
As per Term Project requirements, four separate Fractals must be generated with the following values of `c`: 
* `c = -1`
* `c = 0.3-0.4j`
* `c = 0.360284+0.100376j`
* `c = -0.1+0.8j`