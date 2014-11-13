function [file] = first_file(dir)

filelist = readdir(dir);

for ii = 1:numel(filelist)
	
	f = filelist{ii};
	
	if (regexp (f, "^\\.\\.?$"))
		continue;
	endif
	
	file = [dir f];
	
	break;
	
end

end