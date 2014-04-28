from setuptools import setup

setup(
    name="deferred2",
    version='0.0.1',
    description='Successor of the deferred library shipped with Google AppEngine (GAE)',
    long_description=open('README.rst').read(),
    license='MIT',
    author='herr kaste',
    author_email='herr.kaste@gmail.com',
    url='https://github.com/kaste/deferred2',
    platforms=['linux', 'osx', 'win32'],
    packages = ['deferred2'],
    zip_safe=False,
    classifiers=[
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: POSIX',
    'Operating System :: Microsoft :: Windows',
    'Operating System :: MacOS :: MacOS X',
    'Topic :: Utilities',
    'Programming Language :: Python',
    ],
)