from setuptools import setup


setup(name='tshistory_client',
      version='0.1.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr',
      url='https://bitbucket.org/pythonian/tshistory_client',
      description='timeseries histories python client (through tshistory_rest)',

      packages=['tshistory_client'],
      install_requires=[
          'requests',
          'pandas ~= 0.23.3',
          'pytest_sa_pg',
      ],
      tests_require=[
          'responses',
          'tshistory',
          'tshistory_rest'
      ],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
