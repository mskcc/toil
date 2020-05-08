# Copyright (C) 2015-2020 Regents of the University of California
# Copyright (C) 2015 Curoverse, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import os
import sys
import unittest
import re
import logging
import shutil
import zipfile
import pytest
import uuid
from urllib.request import urlretrieve
from io import StringIO

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

logger = logging.getLogger(__name__)
import subprocess
from toil.test import (ToilTest, needs_cwl, slow, needs_docker, needs_lsf,
                       needs_mesos, needs_parasol, needs_gridengine, needs_slurm,
                       needs_torque)


@needs_cwl
class CWLv10Test(ToilTest):
    def setUp(self):
        """Runs anew before each test to create farm fresh temp dirs."""
        self.outDir = f'/tmp/toil-cwl-test-{str(uuid.uuid4())}'
        os.makedirs(self.outDir)
        self.rootDir = self._projectRootPath()
        self.cwlSpec = os.path.join(self.rootDir, 'src/toil/test/cwl/spec')
        self.workDir = os.path.join(self.cwlSpec, 'v1.0')
        # The latest cwl git commit hash from https://github.com/common-workflow-language/common-workflow-language.
        # Update it to get the latest tests.
        testhash = '40fcfc01812046f012acf5153cc955ee848e69e3' # Date:   Tue Jan 21 07:36:37 2020 +0100
        url = 'https://github.com/common-workflow-language/common-workflow-language/archive/%s.zip' % testhash
        if not os.path.exists(self.cwlSpec):
            urlretrieve(url, 'spec.zip')
            with zipfile.ZipFile('spec.zip', 'r') as z:
                z.extractall()
            shutil.move('common-workflow-language-%s' % testhash, self.cwlSpec)
            os.remove('spec.zip')

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    def _tester(self, cwlfile, jobfile, expect, main_args=[], out_name="output"):
        from toil.cwl import cwltoil
        st = StringIO()
        main_args = main_args[:]
        main_args.extend(['--outdir', self.outDir,
                          os.path.join(self.rootDir, cwlfile), os.path.join(self.rootDir, jobfile)])
        cwltoil.main(main_args, stdout=st)
        out = json.loads(st.getvalue())
        out[out_name].pop("http://commonwl.org/cwltool#generation", None)
        out[out_name].pop("nameext", None)
        out[out_name].pop("nameroot", None)
        self.assertEqual(out, expect)

    def _debug_worker_tester(self, cwlfile, jobfile, expect):
        from toil.cwl import cwltoil
        st = StringIO()
        cwltoil.main(['--debugWorker', '--outdir', self.outDir,
                     os.path.join(self.rootDir, cwlfile),
                     os.path.join(self.rootDir, jobfile)], stdout=st)
        out = json.loads(st.getvalue())
        out["output"].pop("http://commonwl.org/cwltool#generation", None)
        out["output"].pop("nameext", None)
        out["output"].pop("nameroot", None)
        self.assertEqual(out, expect)

    def revsort(self, cwl_filename, tester_fn):
        tester_fn('src/toil/test/cwl/' + cwl_filename,
                  'src/toil/test/cwl/revsort-job.json',
                  self._expected_revsort_output(self.outDir))

    def download(self,inputs, tester_fn):
        input_location = os.path.join('src/toil/test/cwl', inputs)
        tester_fn('src/toil/test/cwl/download.cwl',
                  input_location,
                  self._expected_download_output(self.outDir))

    def test_run_revsort(self):
        self.revsort('revsort.cwl', self._tester)

    def test_run_revsort2(self):
        self.revsort('revsort2.cwl', self._tester)

    def test_run_revsort_debug_worker(self):
        self.revsort('revsort.cwl', self._debug_worker_tester)

    def test_run_s3(self):
        self.download('download_s3.json', self._tester)

    def test_run_http(self):
        self.download('download_http.json', self._tester)

    def test_run_https(self):
        self.download('download_https.json', self._tester)

    @slow
    def test_bioconda(self):
        self._tester('src/toil/test/cwl/seqtk_seq.cwl',
                     'src/toil/test/cwl/seqtk_seq_job.json',
                     self._expected_seqtk_output(self.outDir),
                     main_args=["--beta-conda-dependencies"],
                     out_name="output1")

    @needs_docker
    def test_biocontainers(self):
        self._tester('src/toil/test/cwl/seqtk_seq.cwl',
                     'src/toil/test/cwl/seqtk_seq_job.json',
                     self._expected_seqtk_output(self.outDir),
                     main_args=["--beta-use-biocontainers"],
                     out_name="output1")

    @slow
    def test_restart(self):
        """Enable restarts with toil-cwl-runner -- run failing test, re-run correct test."""
        logger.info('Running CWL Test Restart.  Expecting failure, then success.')
        from toil.cwl import cwltoil
        from toil.jobStores.abstractJobStore import NoSuchJobStoreException
        from toil.leader import FailedJobsException
        outDir = self._createTempDir()
        cwlDir = os.path.join(self._projectRootPath(), "src", "toil", "test", "cwl")
        cmd = ['--outdir', outDir, '--jobStore', os.path.join(outDir, 'jobStore'), "--no-container",
               os.path.join(cwlDir, "revsort.cwl"), os.path.join(cwlDir, "revsort-job.json")]

        # create a fake rev bin that actually points to the "date" binary
        cal_path = [d for d in os.environ["PATH"].split(':') if os.path.exists(os.path.join(d, 'date'))][-1]
        os.symlink(os.path.join(cal_path, 'date'), f'{os.path.join(outDir, "rev")}')

        def path_with_bogus_rev():
            # append to the front of the PATH so that we check there first
            return f'{outDir}:' + os.environ["PATH"]

        orig_path = os.environ["PATH"]
        # Force a failure by trying to use an incorrect version of `rev` from the PATH
        os.environ["PATH"] = path_with_bogus_rev()
        try:
            cwltoil.main(cmd)
            self.fail("Expected problem job with incorrect PATH did not fail")
        except FailedJobsException:
            pass
        # Finish the job with a correct PATH
        os.environ["PATH"] = orig_path
        cwltoil.main(["--restart"] + cmd)
        # Should fail because previous job completed successfully
        try:
            cwltoil.main(["--restart"] + cmd)
            self.fail("Restart with missing directory did not fail")
        except NoSuchJobStoreException:
            pass

    @slow
    @pytest.mark.timeout(2400)
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(2400)
    def test_run_conformance(self, batchSystem=None, caching=False):
        try:
            cmd = ['cwltest', '--tool', 'toil-cwl-runner', '--test=conformance_test_v1.0.yaml',
                   '--timeout=2400', '--basedir=' + self.workDir]
            if batchSystem:
                cmd.extend(["--batchSystem", batchSystem])
            if caching:
                cmd.extend(['--', '--disableCaching="False"'])
            subprocess.check_output(cmd, cwd=self.workDir, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            only_unsupported = False
            # check output -- if we failed but only have unsupported features, we're okay
            p = re.compile(r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features")

            error_log = e.output.decode('utf-8')
            for line in error_log.split('\n'):
                m = p.search(line)
                if m:
                    if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                        only_unsupported = True
                        break
            if not only_unsupported:
                print(error_log)
                raise e

    @slow
    @needs_lsf
    @unittest.skip
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_lsf_cwl_conformance_with_caching(self):
        return self.test_run_conformance(batchSystem="LSF", caching=True)

    @slow
    @needs_slurm
    @unittest.skip
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_slurm_cwl_conformance_with_caching(self):
        return self.test_run_conformance(batchSystem="Slurm", caching=True)

    @slow
    @needs_torque
    @unittest.skip
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_torque_cwl_conformance_with_caching(self):
        return self.test_run_conformance(batchSystem="Torque", caching=True)

    @slow
    @needs_gridengine
    @unittest.skip
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_gridengine_cwl_conformance_with_caching(self):
        return self.test_run_conformance(batchSystem="gridEngine", caching=True)

    @slow
    @needs_mesos
    @unittest.skip
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_mesos_cwl_conformance_with_caching(self):
        return self.test_run_conformance(batchSystem="mesos", caching=True)

    @slow
    @needs_parasol
    @unittest.skip
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_parasol_cwl_conformance_with_caching(self):
        return self.test_run_conformance(batchSystem="parasol", caching=True)

    @slow
    @needs_lsf
    @unittest.skip
    def test_lsf_cwl_conformance(self):
        return self.test_run_conformance(batchSystem="LSF")

    @slow
    @needs_slurm
    @unittest.skip
    def test_slurm_cwl_conformance(self):
        return self.test_run_conformance(batchSystem="Slurm")

    @slow
    @needs_torque
    @unittest.skip
    def test_torque_cwl_conformance(self):
        return self.test_run_conformance(batchSystem="Torque")

    @slow
    @needs_gridengine
    @unittest.skip
    def test_gridengine_cwl_conformance(self):
        return self.test_run_conformance(batchSystem="gridEngine")

    @slow
    @needs_mesos
    @unittest.skip
    def test_mesos_cwl_conformance(self):
        return self.test_run_conformance(batchSystem="mesos")

    @slow
    @needs_parasol
    @unittest.skip
    def test_parasol_cwl_conformance(self):
        return self.test_run_conformance(batchSystem="parasol")

    @staticmethod
    def _expected_seqtk_output(outDir):
        # Having unicode string literals isn't necessary for the assertion but
        # makes for a less noisy diff in case the assertion fails.
        loc = 'file://' + os.path.join(outDir, 'out')
        return {
            'output1':  {
                'location': loc,
                'checksum': 'sha1$322e001e5a99f19abdce9f02ad0f02a17b5066c2',
                'basename': 'out',
                'class': 'File',
                'size': 150}}

    @staticmethod
    def _expected_revsort_output(outDir):
        # Having unicode string literals isn't necessary for the assertion but
        # makes for a less noisy diff in case the assertion fails.
        loc = 'file://' + os.path.join(outDir, 'output.txt')
        return {
            'output': {
                'location': loc,
                'basename': 'output.txt',
                'size': 1111,
                'class': 'File',
                'checksum': 'sha1$b9214658cc453331b62c2282b772a5c063dbd284'}}

    @staticmethod
    def _expected_download_output(outDir):
        # Having unicode string literals isn't necessary for the assertion but
        # makes for a less noisy diff in case the assertion fails.
        loc = 'file://' + os.path.join(outDir, 'output.txt')
        return {
            'output': {
                'location': loc,
                'basename': 'output.txt',
                'size': 0,
                'class': 'File',
                'checksum': 'sha1$da39a3ee5e6b4b0d3255bfef95601890afd80709'}}


@needs_cwl
class CWLv11Test(ToilTest):
    @classmethod
    def setUpClass(cls):
        """Runs anew before each test to create farm fresh temp dirs."""
        cls.outDir = f'/tmp/toil-cwl-v1_1-test-{str(uuid.uuid4())}'
        os.makedirs(cls.outDir)
        cls.rootDir = cls._projectRootPath()
        cls.cwlSpec = os.path.join(cls.rootDir, 'src/toil/test/cwl/spec_v11')
        cls.test_yaml = os.path.join(cls.cwlSpec, 'conformance_tests.yaml')
        # TODO: Use a commit zip in case someone decides to rewrite master's history?
        url = 'https://github.com/common-workflow-language/cwl-v1.1.git'
        commit = 'a22b7580c6b50e77c0a181ca59d3828dd5c69143'
        p = subprocess.Popen(f'git clone {url} {cls.cwlSpec} && cd {cls.cwlSpec} && git checkout {commit}', shell=True)
        p.communicate()

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(2400)
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(2400)
    def test_run_conformance(self, batchSystem=None, caching=False):
        try:
            # TODO: we do not currently pass tests: 213, 236, 242, 243, 244, 245, 246, 249
            selected_tests = '1-212,214-235,237-241,247-248,250-253'
            cmd = [f'cwltest',
                   f'--tool=toil-cwl-runner',
                   f'--test={self.test_yaml}',
                   f'--timeout=2400',
                   f'--basedir={self.cwlSpec}',
                   f'-n={selected_tests}']
            if batchSystem:
                cmd.extend(["--batchSystem", batchSystem])
            if caching:
                cmd.extend(['--', '--disableCaching="False"'])
            subprocess.check_output(cmd, cwd=self.cwlSpec, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            only_unsupported = False
            # check output -- if we failed but only have unsupported features, we're okay
            p = re.compile(r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features")

            error_log = e.output.decode('utf-8')
            for line in error_log.split('\n'):
                m = p.search(line)
                if m:
                    if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                        only_unsupported = True
                        break
            if not only_unsupported:
                print(error_log)
                raise e

@needs_cwl
class CWLv12Test(ToilTest):
    @classmethod
    def setUpClass(cls):
        """Runs anew before each test to create farm fresh temp dirs."""
        cls.outDir = f'/tmp/toil-cwl-v1_2-test-{str(uuid.uuid4())}'
        os.makedirs(cls.outDir)
        cls.rootDir = cls._projectRootPath()
        cls.cwlSpec = os.path.join(cls.rootDir, 'src/toil/test/cwl/spec_v12')
        cls.test_yaml = os.path.join(cls.cwlSpec, 'conformance_tests.yaml')
        # TODO: Use a commit zip in case someone decides to rewrite master's history?
        url = 'https://github.com/common-workflow-language/cwl-v1.2.git'
        commit = 'fca122ef126d03da8c8091111ef5d0cf75763382'
        p = subprocess.Popen(f'git clone {url} {cls.cwlSpec} && cd {cls.cwlSpec} && git checkout {commit}', shell=True)
        p.communicate()

    def tearDown(self):
        """Clean up outputs."""
        if os.path.exists(self.outDir):
            shutil.rmtree(self.outDir)
        unittest.TestCase.tearDown(self)

    @slow
    @pytest.mark.timeout(2400)
    # Cannot work until we fix https://github.com/DataBiosphere/toil/issues/2801
    @pytest.mark.xfail
    def test_run_conformance_with_caching(self):
        self.test_run_conformance(caching=True)

    @slow
    @pytest.mark.timeout(2400)
    def test_run_conformance(self, batchSystem=None, caching=False):
        try:
            # TODO: we do not currently pass tests: 213, 236, 242, 243, 244, 245, 246, 249
            selected_tests = '1-212,214-235,237-241,247-248,250-276'
            cmd = [f'cwltest',
                   f'--tool=toil-cwl-runner',
                   f'--test={self.test_yaml}',
                   f'--timeout=2400',
                   f'--basedir={self.cwlSpec}',
                   f'-n={selected_tests}']
            if batchSystem:
                cmd.extend(["--batchSystem", batchSystem])

            args_passed_directly_to_toil = ['--enable-dev']
            if caching:
                args_passed_directly_to_toil.extend(['--disableCaching="False"'])
            cmd.extend(['--'] + args_passed_directly_to_toil)

            subprocess.check_output(cmd, cwd=self.cwlSpec, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            only_unsupported = False
            # check output -- if we failed but only have unsupported features, we're okay
            p = re.compile(r"(?P<failures>\d+) failures, (?P<unsupported>\d+) unsupported features")

            error_log = e.output.decode('utf-8')
            for line in error_log.split('\n'):
                m = p.search(line)
                if m:
                    if int(m.group("failures")) == 0 and int(m.group("unsupported")) > 0:
                        only_unsupported = True
                        break
            if not only_unsupported:
                print(error_log)
                raise e
