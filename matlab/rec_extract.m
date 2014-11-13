function [] = rec_extract(outfile,filename,frame)

data = load(filename);

X = data.ss0(frame).X;
Y = data.ss0(frame).Y;
Z = data.ss0(frame).Z;
I = data.ss0(frame).intenSR;

save(outfile,"-mat","X","Y","Z","I")

end