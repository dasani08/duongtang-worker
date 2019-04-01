from setuptools import setup, find_packages

setup(
    name='r',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'SQLAlchemy',
        'PyMySQL',
        'celery'
    ]
)
