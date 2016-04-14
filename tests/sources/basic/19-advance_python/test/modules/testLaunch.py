import unittest
import modules.maqueta
import modules.maqueta3
import modules.maqueta5
import modules.maqueta7
import modules.maqueta8
import os
import time


class testLaunch(unittest.TestCase):

    def setUp(self):
        self.app = os.path.abspath(
            modules.maqueta.__file__).replace('.pyc', '.py')

    def test_launch_simple(self):
        """ Aplicacion que utiliza la funcionalidad de launch_pycompss_applicacion
            llamando a aplicaciones externas sin parametros (de distinto fichero! sin anidamiento)
            __main__ --> launch(maqueta.main)|--------> task(function_A)
        """
        time.sleep(20)
        from pycompss.runtime.launch import launch_pycompss_application
        print self.app
        app = os.path.abspath(modules.maqueta.__file__).replace('.pyc', '.py')
        x = launch_pycompss_application(app, 'main')
        print x
        self.assertEqual(x, 6)

    def test_launch_2(self):
        """ Aplicacion que utiliza la funcionalidad de launch_pycompss_applicacion
            llamando a aplicaciones con parametros (de distinto fichero! sin anidamiento)
            __main__ --> launch(app) |--------> task(function_A)
        """
        time.sleep(20)
        from pycompss.runtime.launch import launch_pycompss_application
        args = ['1', '2', '3']
        kwargs = {}
        app = os.path.abspath(modules.maqueta3.__file__).replace('.pyc', '.py')
        x = launch_pycompss_application(app, 'app', args, kwargs)
        print x
        self.assertEqual(x, str(123))

    @unittest.skip("no pueden lanzarse todoos")
    def test_launch_task_launch_task(self):
        """ Aplicacion que utiliza la funcionalidad de launch_pycompss_applicacion
            llamando a aplicaciones con parametros y con anidamiento
            (distintos ficheros!)

            __main__ --> launch(app)|--------> task(function_A)
                                             -----> launch(auxiliar.app2)
                                                        |----------> task(function_B)"""
        from pycompss.runtime.launch import launch_pycompss_application
        app = os.path.abspath(modules.maqueta5.__file__).replace('.pyc', '.py')
        args = ['1', '2', '3']
        kwargs = {}
        x = launch_pycompss_application(app, 'app', args, kwargs)
        print x
        self.assertEqual(x, str(123))

    @unittest.skip("no pueden lanzarse todoos")
    def test_launch_task_launch_task_2(self):
        from pycompss.runtime.launch import launch_pycompss_application
        app_path = os.path.abspath(modules.maqueta7.__file__).replace('.pyc', '.py')
        args = ['1', '2', '3']
        kwargs = {}
        x = launch_pycompss_application(app_path, 'app', args, kwargs)
        print x
        self.assertEqual(x, str(123))

    @unittest.skip("no pueden lanzarse todoos")
    def test_launch_params(self):
        from pycompss.runtime.launch import launch_pycompss_application
        app_path = "/home/user/test_maqueta/src/maqueta8.py"
        args = ['1', '2', '3']
        kwargs = {}
        x = launch_pycompss_application(app_path, 'app', args, kwargs,
                                        debug=False,
                                        project_xml='/home/user/test_maqueta/xml/project.xml',
                                        resources_xml='/home/user/test_maqueta/xml/resources.xml',
                                        comm='NIO')
        print x

    def tearDown(self):
        self.path = None
