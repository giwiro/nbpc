from setuptools import setup, find_packages

setup(name="nbpc",
      version="0.0.1",
      description="Name Based Product Categorization",
      url="http://github.com/giwiro/nbpc",
      author="Gi Wah Davalos Loo",
      author_email="giwirodavalos@gmail.com",
      license="MIT",
      packages=find_packages(),
      install_requires=["pymongo", "mysql-connector-python", "boto3", "pandas", "tqdm"])
