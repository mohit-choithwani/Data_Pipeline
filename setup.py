from setuptools import setup, find_packages
import os

def get_requirements(requirements_path):
    """Get the requirements from the requirements.txt file
        It removes the \n and -e . from the file.
    Args:
        requirements_path : str : path to the requirements.txt file
    Returns:
        list : list of requirements
    """
    
    requirements = []
    HYPHEN_E = '-e .'
    
    # Use a relative path here to make it portable
    with open(requirements_path) as f:
        requirements = f.readlines()
        
    # Clean the requirements list
    requirements = [req.replace("\n", "") for req in requirements if req.strip() and req != HYPHEN_E]
    
    return requirements

# Use relative path for requirements.txt and other files
requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')

setup(
    name='SeaBreeze-Project',
    version='0.0.1',
    packages=find_packages(),  # Automatically find all packages in the directory
    install_requires=get_requirements(requirements_path),  # Use the relative path for the requirements
    author='Mohit',
    author_email='mohit.choithwani.97@gmail.com',
    description='Real-time data processing and analysis pipeline',
    long_description_content_type='text/markdown',  # For markdown formatting
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6', 
)