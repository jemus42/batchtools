test_that("cf processx basic", {
  skip_on_os("windows")
  skip_if_not_installed("processx")

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsProcessx(2, fs.latency = 0)
  ids = batchMap(Sys.sleep, time = c(2, 2), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.2, expire.after = 1, reg = reg))
  })
  expect_data_table(findOnSystem(reg = reg), nrows = 0)
  expect_equal(findDone(reg = reg), findJobs(reg = reg))
})

test_that("cf processx max.concurrent.jobs", {
  skip_on_os("windows")
  skip_if_not_installed("processx")

  # check that max.concurrent.jobs works
  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsProcessx(2, fs.latency = 0)
  reg$max.concurrent.jobs = 1
  ids = batchMap(Sys.sleep, time = c(2, 0), reg = reg)
  submitAndWait(1:2, reg = reg)
  tab = getJobStatus(reg = reg)
  expect_true(diff(tab$started) > 1)
})

test_that("cf processx kill jobs", {
  skip_on_os("windows")
  skip_if_not_installed("processx")

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsProcessx(4, fs.latency = 0)
  ids = batchMap(Sys.sleep, time = rep(30, 4), reg = reg)
  silent({
    submitJobs(reg = reg)
  })
  Sys.sleep(1)

  # Should have 4 running jobs
  expect_data_table(findRunning(reg = reg), nrows = 4)

  # Kill one job
  silent({
    killJobs(ids = 1, reg = reg)
  })
  Sys.sleep(0.5)

  # Should have 3 running jobs
  expect_data_table(findRunning(reg = reg), nrows = 3)

  # Kill remaining jobs
  silent({
    killJobs(reg = reg)
  })
  Sys.sleep(0.5)

  # Should have 0 running jobs
  expect_data_table(findRunning(reg = reg), nrows = 0)
})

test_that("cf processx pid files persist", {
  skip_on_os("windows")
  skip_if_not_installed("processx")

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsProcessx(2, fs.latency = 0)
  ids = batchMap(Sys.sleep, time = c(5, 5), reg = reg)
  silent({
    submitJobs(reg = reg)
  })
  Sys.sleep(1)

  # Check PID files are created
  pid_dir = file.path(reg$file.dir, "processx_pids")
  expect_true(dir.exists(pid_dir))
  pid_files = list.files(pid_dir, pattern = "\\.pid$")
  expect_equal(length(pid_files), 2)

  # Clean up
  silent({
    killJobs(reg = reg)
  })
  Sys.sleep(0.5)

  # PID files should be removed after kill
  pid_files = list.files(pid_dir, pattern = "\\.pid$")
  expect_equal(length(pid_files), 0)
})
